# -*- coding: utf-8 -*-

import os
import pandas as pd
import dask.dataframe as dd
from ddf_utils.factory import ILOLoader
from ddf_utils.str import to_concept_id, format_float_digits
from pandas.api.types import CategoricalDtype


ilo = ILOLoader()


def get_all_column_names(indicators):
    sets = set()

    for i in indicators:
        df = next(pd.read_csv(f'../source/{i}.csv.gz', chunksize=1))
        for c in df.columns:
            sets.add(c)

    return sets


def check_columns(df):
    df_cols = set(df.columns)
    columns_should_appear = ['classif1', 'classif2', 'collection', 'indicator',
                             'note_classif', 'note_indicator', 'note_source', 'obs_status',
                             'obs_value', 'ref_area', 'sex', 'source', 'time', "note_indicator", "note_classif"]

    if not df_cols.issubset(set(columns_should_appear)):
        print("expected columns: {}".format(columns_should_appear))
        print("but actually columns: {}".format(df_cols))
        raise ValueError("there are unexpected columns in the dataframe")


def read_sources(indicators):
    # read all indicators data from file
    colsToServe = ['indicator', 'ref_area', 'sex', 'classif1', 'classif2', 'time', 'obs_value']

    for i in indicators:
        print("loading indicator: ", i)
        try:
            df = pd.read_csv(f'../source/{i}.csv.gz')
        except FileNotFoundError:
            print(f"file not found: {i}")
            return
        check_columns(df)
        cts = colsToServe.copy()
        # add `source` and `note_source` column, to help removing duplicated datapoints
        cts.append('source')
        for ta in ['note_source', 'note_classif', 'note_indicator']:
            if ta in df.columns:
                cts.append(ta)
        for c in colsToServe:
            if c not in df.columns:
                cts.remove(c)
        try:
            yield (i, df[cts])
        except KeyError:
            print(cts)
            print(df.columns.tolist())
            raise


def serve_datapoints(df, indicator_name, by, split_entity=None):
    if not split_entity:
        fn = 'ddf--datapoints--' + indicator_name + '--by--' + '--'.join(by) + '.csv'
        df.to_csv(f'../../{fn}', index=False)
        return

    assert type(split_entity) is str, "only support split by one column."
    dir_name = 'ddf--datapoints--' + indicator_name + '--by--' + '--'.join(by)
    os.makedirs(os.path.join('../../', dir_name), exist_ok=True)
    grouped = df.groupby(by=split_entity)
    split_entity_idx = by.index(split_entity)
    for g, df_group in grouped:
        by_new = by.copy()
        by_new[split_entity_idx] = by_new[split_entity_idx] + '-' + g
        fn = 'ddf--datapoints--' + indicator_name + '--by--' + '--'.join(by_new) + '.csv'
        df_group.to_csv(os.path.join('../../', dir_name, fn), index=False)


def create_datapoints(dfs):
    # prepare the source/note_source category
    source = ilo.load_metadata(table='source').source.values
    note_source = ilo.load_metadata(table='note_source').note_source.values
    source_cat = CategoricalDtype(categories=source, ordered=True)
    note_source_cat = CategoricalDtype(categories=note_source, ordered=True)
    # categories = {'source': source_cat, 'note_source': note_source_cat}

    # create ddf datapoints
    for df in dfs.values():
        iName = to_concept_id(df.indicator.unique()[0])

        df = df.drop('indicator', axis=1).rename(columns={'obs_value': iName})

        by = df.columns.tolist()
        by.remove(iName)
        by.remove('source')

        for ta in ['note_source', 'note_indicator', 'note_classif']:
            if ta in by:
                by.remove(ta)

        for c in by:
            if c in ['ref_area', 'sex', 'classif1', 'classif2']:
                df[c] = df[c].map(to_concept_id)

        # removing rows where "classif1" or "classif2" is
        # empty. Because these columns are not suppose to be empty.
        for c in ['classif1' , 'classif2']:
            if c in df.columns and df[c].hasnans:
                print(f"{iName}: nan found in column {c}, dropping them")
                df = df[~pd.isnull(df[c])]

        # removing duplicates.
        # duplications are due to different sources.  The source.csv
        # and note_source.csv files from the ILO Stat bulk download
        # are sorted and it looks like they are sorted by data
        # quality. So we sort the dataframe by note_source and use the
        # first datapoint.
        df = df.dropna(subset=[iName])

        if df.duplicated(subset=by).any():

            if 'source' in df.columns:
                # source column: we only keep one source for a country, to keep consistency for the series
                df['source'] = df['source'].astype(source_cat)
                groups = df.groupby(by=['ref_area'])
                to_concat = list()
                for _, area_df in groups:
                    first_source = area_df.source.sort_values().values[0]
                    to_concat.append(area_df[area_df.source ==  first_source])
                df = pd.concat(to_concat)
                # print(df.columns)

                if 'note_source' in df.columns:
                    df['note_source'] = df['note_source'].astype(note_source_cat)
                    df = df.sort_values(by=['note_source']).drop_duplicates(subset=by, keep='first')

                if df.duplicated(subset=by).any():
                    print(f"there are still duplicated datapoints in {iName} after selecting source/note_source\n"
                          "keeping the first value")
                    df = df.drop_duplicates(subset=by, keep='first')
            else:
                print(f"there are duplicated datapoints in {iName}, but no source/note_source column")

        # debugging duplicated datapoint issue
        # if df.duplicated(subset=by).any():
        #     import ipdb
        #     ipdb.set_trace()

        for c in ['source', 'note_source', 'note_indicator', 'note_classif']:
            if c in df.columns:
                df = df.drop([c], axis=1)

        # df = df.dropna(how='any')
        df[iName] = df[iName].map(format_float_digits)
        df['time'] = df['time'].astype('int16')
        df = df.sort_values(by=by)
        if df.shape[0] > 150000:
            serve_datapoints(df, iName, by, split_entity='ref_area')
        else:
            serve_datapoints(df, iName, by)


def non_arg_emp_indicators():
    """create following indicators, which is requried by SG.

    - ees_mnag_noc_rt: Share of paid employment in non-agricultural employment, men (%)
    - ees_fnag_noc_rt: Share of paid employment in non-agricultural employment, women (%)
    - ees_tnag_noc_rt: Share of paid employment in non-agricultural employment (%)

    select Employment and then Employees by sex and economic activity,
    you just need to unselect Agriculture.  You can do the same for
    Employment by sex and economic activity.  And then it's the ratio
    of both.
    """
    employment = dd.read_csv('../../ddf--datapoints--emp_temp_sex_eco_nb--by--ref_area--sex--classif1--time/*.csv').compute()
    employees = dd.read_csv('../../ddf--datapoints--ees_tees_sex_eco_nb--by--ref_area--sex--classif1--time/*.csv').compute()

    # we use eco_sector_nag as filter for non agricultural data
    emp = employment[employment.classif1.isin(['eco_sector_nag'])].drop('classif1', axis=1)
    empee = employees[employees.classif1.isin(['eco_sector_nag'])].drop('classif1', axis=1)

    emp = emp.set_index(['ref_area', 'sex', 'time'])['emp_temp_sex_eco_nb']
    empee = empee.set_index(['ref_area', 'sex', 'time'])['ees_tees_sex_eco_nb']
    emp1, empee1 = emp.align(empee)  # align the index

    res = empee1 / emp1 * 100
    res = res.dropna().reset_index()

    sex_indicator_mapping = {'sex_m': 'ees_mnag_noc_rt', 'sex_f': 'ees_fnag_noc_rt', 'sex_t': 'ees_tnag_noc_rt'}

    for sex, indicator in sex_indicator_mapping.items():
        df = res[res.sex == sex].drop('sex', axis=1)
        df.columns = ['ref_area', 'time', indicator]
        df[indicator] = df[indicator].map(format_float_digits)
        df.to_csv(f'../../ddf--datapoints--{indicator}--by--ref_area--time.csv', index=False)


def create_entities():
    # entities
    classif1 = ilo.load_metadata('classif1')
    classif2 = ilo.load_metadata('classif2')
    ref_area = ilo.load_metadata('ref_area')
    sex = ilo.load_metadata('sex')

    discreteConcepts = set()

    sex['sex'] = sex['sex'].map(to_concept_id)
    sex.columns = sex.columns.map(lambda x: x.strip().replace('.', '_'))
    [discreteConcepts.add(x) for x in sex.columns]
    sex.to_csv('../../ddf--entities--sex.csv', index=False)

    ref_area['ref_area'] = ref_area['ref_area'].map(to_concept_id)
    ref_area.columns = ref_area.columns.map(lambda x: x.strip().replace('.', '_'))
    [discreteConcepts.add(x) for x in ref_area.columns]
    ref_area.to_csv('../../ddf--entities--ref_area.csv', index=False)

    classif1['classif1'] = classif1['classif1'].map(to_concept_id)
    classif1.columns = classif1.columns.map(lambda x: x.strip().replace('.', '_'))
    [discreteConcepts.add(x) for x in classif1.columns]
    classif1.to_csv('../../ddf--entities--classif1.csv', index=False)

    classif2['classif2'] = classif2['classif2'].map(to_concept_id)
    classif2.columns = classif2.columns.map(lambda x: x.strip().replace('.', '_'))
    [discreteConcepts.add(x) for x in classif2.columns]
    classif2.to_csv('../../ddf--entities--classif2.csv', index=False)

    return discreteConcepts


def non_arg_emp_indicators_concepts():
    """ create dataframe for following:

    - ees_mnag_noc_rt: Share of paid employment in non-agricultural employment, men (%)
    - ees_fnag_noc_rt: Share of paid employment in non-agricultural employment, women (%)
    - ees_tnag_noc_rt: Share of paid employment in non-agricultural employment (%)
    """
    data = [['ees_mnag_noc_rt', 'Share of paid employment in non-agricultural employment, men (%)'],
            ['ees_fnag_noc_rt', 'Share of paid employment in non-agricultural employment, women (%)'],
            ['ees_tnag_noc_rt', 'Share of paid employment in non-agricultural employment (%)']]
    res = pd.DataFrame(data, columns=['concept', 'concept_label'])
    res['concept_type'] = 'measure'
    return res


def create_concepts(discreteConcepts):
    md = ilo.load_metadata()
    # concepts
    md = md.set_index('id')
    md['indicator'] = md['indicator'].map(to_concept_id)
    md.columns = md.columns.map(lambda x: x.strip().replace('.', '_'))
    md = md[md.freq == 'A']  # only Annual indicators
    md = md.drop(['size', 'freq', 'freq_label'], axis=1)
    md = md.rename(columns={'indicator': 'concept', 'indicator_label': 'concept_label'})
    md['concept_type'] = 'measure'
    md['n_records'] = md['n_records'].map(str)
    md = md.append(non_arg_emp_indicators_concepts(), ignore_index=True, sort=False)
    md = md.sort_values(by='concept')
    md.to_csv('../../ddf--concepts--continuous.csv', index=False)

    [discreteConcepts.add(x) for x in md.columns if x != 'concept_type']
    discreteDF = pd.DataFrame(pd.Series(list(discreteConcepts)))
    discreteDF.columns = ['concept']

    discreteDF['concept_label'] = discreteDF['concept'].map(lambda x: x.replace('_', ' ').title())

    discreteDF = discreteDF.set_index('concept')
    discreteDF.loc['concept_label'] = ['Concept Label']

    discreteDF['concept_type'] = 'string'
    discreteDF.loc[['sex', 'ref_area', 'classif1', 'classif2'], 'concept_type'] = 'entity_domain'
    discreteDF.loc['time'] = ['Time', 'time']

    discreteDF.sort_index().to_csv('../../ddf--concepts--discrete.csv')


def main():
    md = ilo.load_metadata()
    indicators = md[md['freq'] == 'A']['id'].values
    dfs = dict(read_sources(indicators))
    create_datapoints(dfs)
    discreteConcepts = create_entities()
    create_concepts(discreteConcepts)
    non_arg_emp_indicators()


if __name__ == '__main__':
    main()
    print('done.')
