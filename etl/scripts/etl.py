# coding: utf8

import pandas as pd
from ddf_utils.factory import ilo


def get_all_column_names(indicators):
    sets = set()

    for i in indicators:
        df = next(pd.read_csv(f'../source/{i}.csv.gz', chunksize=1))
        for c in df.columns:
            sets.add(c)

    return sets


def main():
    md = ilo.load_metadata()
    indicators = md[md['freq'] == 'A']['id'].values

    columns_names = get_all_column_names(indicators)
    columns_should_appear = ['classif1', 'classif2', 'collection', 'indicator',
                             'note_classif', 'note_indicator',  'note_source', 'obs_status',
                             'obs_value', 'ref_area', 'sex', 'source', 'time']

    try:
        assert columns_names == set(columns_should_appear)
    except AssertionError:
        print("expected columns: {}".format(columns_should_appear))
        print("but actually columns: {}".format(columns_names))
        raise

    colsToServe = ['indicator', 'ref_area', 'sex', 'classif1', 'classif2', 'time', 'obs_value']

    dfs = {}

    # read all indicators data from file
    for i in indicators:
        print("loading indicator: ", i)
        df = pd.read_csv(f'../source/{i}.csv.gz')
        cts = colsToServe.copy()
        for c in colsToServe:
            if c not in df.columns:
                cts.remove(c)
        try:
            dfs[i] = df[cts]
        except KeyError:
            print(cts)
            print(df.columns.tolist())
            raise

    # create ddf datapoints
    for df in dfs.values():
        df_ = df.copy()

        iName = df_.indicator.unique()[0].lower()

        df_ = df_.drop('indicator', axis=1).rename(columns={'obs_value': iName})

        by = df_.columns.tolist()
        by.remove(iName)

        for c in by:
            if c in ['ref_area', 'sex', 'classif1', 'classif2']:
                df_[c] = df_[c].str.lower()

        fn = 'ddf--datapoints--' + iName + '--by--' + '--'.join(by) + '.csv'

        df_.dropna(how='any').to_csv(f'../../{fn}', index=False)

    # entities
    classif1 = ilo.load_metadata('classif1')
    classif2 = ilo.load_metadata('classif2')
    ref_area = ilo.load_metadata('ref_area')
    sex = ilo.load_metadata('sex')

    discreteConcepts = set()

    sex['sex'] = sex['sex'].str.lower()
    sex.columns = sex.columns.map(lambda x: x.strip().replace('.', '_'))
    [discreteConcepts.add(x) for x in sex.columns]
    sex.to_csv('../../ddf--entities--sex.csv', index=False)

    ref_area['ref_area'] = ref_area['ref_area'].str.lower()
    ref_area.columns = ref_area.columns.map(lambda x: x.strip().replace('.', '_'))
    [discreteConcepts.add(x) for x in ref_area.columns]
    ref_area.to_csv('../../ddf--entities--ref_area.csv', index=False)

    classif1['classif1'] = classif1['classif1'].str.lower()
    classif1.columns = classif1.columns.map(lambda x: x.strip().replace('.', '_'))
    [discreteConcepts.add(x) for x in classif1.columns]
    classif1.to_csv('../../ddf--entities--classif1.csv', index=False)

    classif2['classif2'] = classif2['classif2'].str.lower()
    classif2.columns = classif2.columns.map(lambda x: x.strip().replace('.', '_'))
    [discreteConcepts.add(x) for x in classif2.columns]
    classif2.to_csv('../../ddf--entities--classif2.csv', index=False)

    # concepts
    md = md.set_index('id')
    md['indicator'] = md['indicator'].str.lower()
    md.columns = md.columns.map(lambda x: x.strip().replace('.', '_'))
    md = md[md.freq == 'A']  # only Annual indicators
    md = md.drop(['size', 'freq', 'freq_label'], axis=1)
    md = md.rename(columns={'indicator': 'concept', 'indicator_label': 'concept_label'})
    md.to_csv('../../ddf--concepts--continuous.csv', index=False)

    [discreteConcepts.add(x) for x in md.columns]
    discreteDF = pd.DataFrame(pd.Series(list(discreteConcepts)))
    discreteDF.columns = ['concept']

    discreteDF['concept_label'] = discreteDF['concept'].map(lambda x: x.replace('_', ' ').title())

    discreteDF = discreteDF.set_index('concept')
    discreteDF.loc['concept_label'] = [['Concept Label']]

    discreteDF['concept_type'] = 'string'
    discreteDF.loc[['sex', 'ref_area', 'classif1', 'classif2'], 'concept_type'] = 'entity_domain'
    discreteDF.loc['time'] = ['Time', 'time']

    discreteDF.to_csv('../../ddf--concepts--discrete.csv')


if __name__ == '__main__':
    main()
