from io import StringIO

'''
This libraries versions used are those. So, new versions could have changed some stuffs.
It runs at the CREMI & on my personal computer it works too with those versions
- Python 3.7.3
- numpy version : 1.16.5
- pandas version : 0.23.3
- psycopg2 version : 2.8.6
'''

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras

'''
This file will create the database model.
Firstly, it will delete and create the tables of our model.
Then, it will import all the datas related to our CSV/XLS or model logic (like serial primary keys).
'''

# Try to connect to an existing database
params = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "root"
}


# Connection to the database
def connect(parameters: dict) -> psycopg2.connect:
    conn = None
    try:
        print('Connecting to the database... : ' + str(params["database"]))
        conn = psycopg2.connect(**parameters)
    except Exception as e:
        exit("Impossible to connect at database: " + str(e))
    print('Connected to database : ' + str(params["database"]))
    return conn


# Generic import method without specific treatments or conditions.
def importCSV(filePath: str, fieldsToImport: list, table: str, tableColumns: set, indexing: bool = False) -> None:
    df = pd.read_csv(filePath, skipinitialspace=True, usecols=fieldsToImport)
    df = df[fieldsToImport]  # change column orders like it is specified
    buffer = StringIO()
    df.to_csv(buffer, header=None, index=indexing, sep=',', mode="wb", encoding="UTF-8")
    buffer.seek(0)  # go to 0 index after writing
    cur.copy_from(buffer, table, sep=",", columns=tableColumns)


# Commune needs to not treat COMD type lines, so we will remove them of our dataset then we are gonna import
def importCommune(filePath: str, table: str, tableColumns: set) -> None:
    df = pd.read_csv(filePath, skipinitialspace=True, usecols=['TYPECOM', 'COM', 'DEP', 'LIBELLE'])
    df = df.loc[df['TYPECOM'] == 'COM']
    buffer = StringIO()
    df.to_csv(buffer, header=None, columns=['COM', 'DEP', 'LIBELLE'], index=False, sep=',', mode="wb", encoding="UTF-8")
    buffer.seek(0)
    cur.copy_from(buffer, table, sep=",", columns=tableColumns)


def importChefLieuDpt(filePath: str, table: str, tableColumns: set) -> None:
    df = pd.read_csv(filePath, skipinitialspace=True, usecols=['DEP', 'CHEFLIEU'])
    dfA = df['CHEFLIEU'].values
    dfB = df['DEP'].values

    # getting my own serial key
    res = []
    for chefLieu in dfA:
        query = """SELECT id_com FROM commune WHERE COM=%s"""
        cur.execute(query, (str(chefLieu),))
        res.append(cur.fetchone()[0])

    # in our dataset this could be removed and we can use the first column (ids) of our dataset,
    # but if a line in the future isn't ordered this could be risky, so I prefer to get the serial keys more safely that way
    res2 = []
    for dep in dfB:
        query = """SELECT id_dep FROM departement WHERE DEP=%s"""
        cur.execute(query, (str(dep),))
        res2.append(cur.fetchone()[0])

    # adding it to the dataset
    df['id_com'] = res
    df['id_dep'] = res2

    # removing unused columns
    df.drop(['CHEFLIEU', 'DEP'], axis=1, inplace=True)

    # importing
    buffer = StringIO()
    df.to_csv(buffer, header=None, index=False, sep=',', mode="wb", encoding="UTF-8")
    buffer.seek(0)
    cur.copy_from(buffer, table, sep=',', columns=tableColumns)


def importChefLieuRegion(filePath: str, table: str, tableColumns: set) -> None:
    df = pd.read_csv(filePath, skipinitialspace=True, usecols=['REG', 'CHEFLIEU'])
    dfA = df['CHEFLIEU'].values

    res = []
    for chefLieu in dfA:
        query = """SELECT id_com FROM commune WHERE COM=%s"""
        cur.execute(query, (str(chefLieu),))
        res.append(cur.fetchone()[0])

    df['id_com'] = res
    df.drop('CHEFLIEU', axis=1, inplace=True)
    df = df[['id_com', 'REG']]

    buffer = StringIO()
    df.to_csv(buffer, header=None, index=False, sep=',', mode="wb", encoding="UTF-8")
    buffer.seek(0)
    cur.copy_from(buffer, table, sep=',', columns=tableColumns)


def importPopulations(filePath: str, table: str, tableColumns: set) -> None:
    # reading the file
    # precision of dtype to avoid typing confusion of pandas and avoid the lazy solution that is low_memory = False as a bad practice
    df = pd.read_csv(filePath, skipinitialspace=True, sep=';',
                     dtype={'CODGEO': 'str', 'P18_POP': 'int', 'P13_POP': 'int', 'P08_POP': 'int', 'D99_POP': 'int',
                            'D90_POP': 'int', 'D82_POP': 'int', 'D75_POP': 'int', 'D68_POP': 'int'},
                     usecols=['CODGEO', 'P18_POP', 'P13_POP', 'P08_POP', 'D99_POP', 'D90_POP', 'D82_POP', 'D75_POP',
                              'D68_POP'])

    # Get all the commune codes of our database
    query = """SELECT COM FROM commune"""
    cur.execute(query)
    res = cur.fetchall()
    res2 = [row[0] for row in res]

    # Remove national commune codes
    df = df[df['CODGEO'].isin(res2)]

    addTypeAndYearsToDataSet(df)  # adds related years and types to dataset to be able to copy to my model

    labels = {'type2018': 'P18_POP', 'type2013': 'P13_POP', 'type2008': 'P08_POP', 'type1999': 'D99_POP',
              'type1990': 'D90_POP', 'type1982': 'D82_POP', 'type1975': 'D75_POP', 'type1968': 'D68_POP'}

    # Copying population indicators for all the years to LibelleIndicateurs table
    for key in labels:
        copyDatasetTo(df, [key, labels[key]], 'LibelleIndicateurs', ('typelibelle', 'valeur'))

    addComKeysFromLibelleDf(df)  # add com keys to the df

    # Preparing a dataset to copy for the table indicateurCommune
    # What will be complex is to retrieve the SERIAL keys (that can be various, naïvely i could have done a supposition of basic ordering
    # but it makes the copying more complex for different types of indicator, and also, for extensibility it could be more complex.
    # So my implementation motivation is to follow a more a generic approach with our database, we will use requests that will get them back depending on the COM keys of a certain type of libelle

    labels2 = [['P18_POP', 'libelle2018', 'annee2018'], ['P13_POP', 'libelle2013', 'annee2013'],
               ['P08_POP', 'libelle2008', 'annee2008'], ['D99_POP', 'libelle1999', 'annee1999'],
               ['D90_POP', 'libelle1990', 'annee1990'], ['D82_POP', 'libelle1982', 'annee1982'],
               ['D75_POP', 'libelle1975', 'annee1975'], ['D68_POP', 'libelle1968', 'annee1968']]
    labelsToCopyWith = ['id_com', 'annee2018', 'CODGEO', 'libelle2018']
    addComKeysFromLibelleDf(df)

    for sublist in labels2:
        addSerialKeysOfLibelle(df, sublist)
        labelsToCopyWith[1] = sublist[2]
        labelsToCopyWith[3] = sublist[1]
        copyDatasetTo(df, labelsToCopyWith, table, tableColumns)

    ''' 
    # Can be used to reorder digits, but it's already done with pandas reading
    for i in range(len(df['CODGEO'].values)):
        if (len(val) < 5):
            missingDigits = 5 - len(val)
            strToAdd = '0' * missingDigits
            df['CODGEO'].values[i] = ''.join((strToAdd, df['CODGEO'].values[i]))
    '''

# Functions below are used for extensibility or generic/recursivity reasons to implement the copy_from with our specific dataset.
def typeLibelleYears(df):
    df['type2018'] = 'P18_POP'
    df['type2013'] = 'P13_POP'
    df['type2008'] = 'P08_POP'
    df['type1999'] = 'D99_POP'
    df['type1990'] = 'D90_POP'
    df['type1982'] = 'D82_POP'
    df['type1975'] = 'D75_POP'
    df['type1968'] = 'D68_POP'

def addYears(df):
    df['annee2018'] = 2018
    df['annee2013'] = 2013
    df['annee2008'] = 2008
    df['annee1999'] = 1999
    df['annee1990'] = 1990
    df['annee1982'] = 1982
    df['annee1975'] = 1975
    df['annee1968'] = 1968


def addTypeAndYearsToDataSet(df):
    typeLibelleYears(df)
    addYears(df)


def copyDatasetTo(df, datasetColumns: list, table: str, tableColumns: set):
    df2 = df[datasetColumns]
    buffer = StringIO()
    df2.to_csv(buffer, header=None, index=False, sep=',', mode="wb", encoding="UTF-8")
    buffer.seek(0)
    cur.copy_from(buffer, table, sep=',', columns=tableColumns)


def addSerialKeysOfLibelle(df, keys: list):
    query = """SELECT id_libelle FROM libelleindicateurs WHERE typelibelle=%s"""
    cur.execute(query, (keys[0],))
    res = cur.fetchall()
    res2 = [row[0] for row in res]
    df[keys[1]] = res2


def addComKeysFromLibelleDf(df):
    dfA = df['CODGEO'].values
    resB = []
    for com in dfA:
        query = """SELECT id_com FROM commune WHERE COM=%s"""
        cur.execute(query, (str(com),))
        resB.append(cur.fetchone()[0])
    df['id_com'] = resB


def addDepKeysFromLibelleDf(df):
    dfA = df['num'].values
    resB = []
    for dep in dfA:
        query = """SELECT id_dep FROM departement WHERE DEP=%s"""
        cur.execute(query, (str(dep),))
        resB.append(cur.fetchone()[0])
    df['id_dep'] = resB


def getTypesLibelleList(type: str, columnSize: int) -> list:
    typeLibelle = []
    typeLibelle.extend([type for i in range(columnSize)])
    return typeLibelle


# This parsing function is taken from : https://stackoverflow.com/questions/43367805/pandas-read-excel-multiple-tables-on-the-same-sheet
# I used it to be able to parse more easily the xls file with multiple sheets & more with his complex dataframe repartition in them.
def parse_excel_sheet(file, sheet_name=0, threshold=5):
    '''parses multiple tables from an excel sheet into multiple data frame objects. Returns [dfs, df_mds], where dfs is a list of data frames and df_mds their potential associated metadata'''
    xl = pd.ExcelFile(file)
    entire_sheet = xl.parse(sheet_name=sheet_name)

    # count the number of non-Nan cells in each row and then the change in that number between adjacent rows
    n_values = np.logical_not(entire_sheet.isnull()).sum(axis=1)
    n_values_deltas = n_values[1:] - n_values[:-1].values

    # define the beginnings and ends of tables using delta in n_values
    table_beginnings = n_values_deltas > threshold
    table_beginnings = table_beginnings[table_beginnings].index
    table_endings = n_values_deltas < -threshold
    table_endings = table_endings[table_endings].index
    if len(table_beginnings) < len(table_endings) or len(table_beginnings) > len(table_endings) + 1:
        raise BaseException('Could not detect equal number of beginnings and ends')

    # look for metadata before the beginnings of tables
    md_beginnings = []
    for start in table_beginnings:
        md_start = n_values.iloc[:start][n_values == 0].index[-1] + 1
        md_beginnings.append(md_start)

    # make data frames
    dfs = []
    df_mds = []
    for ind in range(len(table_beginnings)):
        start = table_beginnings[ind] + 1
        if ind < len(table_endings):
            stop = table_endings[ind]
        else:
            stop = entire_sheet.shape[0]
        df = xl.parse(sheet_name=sheet_name, skiprows=start, nrows=stop - start)
        dfs.append(df)

        md = xl.parse(sheet_name=sheet_name, skiprows=md_beginnings[ind], nrows=start - md_beginnings[ind] - 1).dropna(
            axis=1)
        df_mds.append(md)
    return dfs, df_mds


def setSocialLibelleLabels(df1, type):
    df1['label1'] = 'Espérance de vie des hommes à la naissance pour les ' + type + ' en 2018'
    df1['label2'] = 'Espérance de vie des hommes à la naissance pour les ' + type + ' en 2010'
    df1['label3'] = 'Espérance de vie des femmes à la naissance pour les ' + type + ' en 2018'
    df1['label4'] = 'Espérance de vie des femmes à la naissance pour les ' + type + ' en 2010'
    df1['label5'] = ' Disparité de niveau de vie pour les ' + type + ' en 2014'
    df1['label6'] = ' Taux de pauvreté pour les ' + type + ' en 2014'
    df1['label7'] = 'Part des jeunes non insérés pour les ' + type + ' en 2014'
    df1['label8'] = 'Part des jeunes non insérés pour les ' + type + ' en 2009'
    df1[
        'label9'] = 'Part de la population éloignée de plus de 7 mn des services de santé de proximité pour les ' + type + ' en 2016'
    df1['label10'] = 'Part de la population estimée en zone inondable pour les ' + type + ' en 2013'
    df1['label11'] = 'Part de la population estimée en zone inondable pour les ' + type + ' en 2008'


def importSocialLibellesFromDataset(df, table: str):
    labels = []
    for i in range(1, len(df.iloc[0])):
        labels.append(df.iloc[0][i])

    df = df.drop(0)  # useless line for our copy, & labels are saved in realCols
    # dfFiltered = df1[df1.columns[~df1.columns.isin(['num', 'libelle'])]]
    if table == 'IndicateurRegion':
        setSocialLibelleLabels(df, 'reg')
    elif table == 'IndicateurDepartement':
        setSocialLibelleLabels(df, 'dep')

    for j in range(2, len(df.keys()) - 11):
        copyDatasetTo(df, ['label' + str(j - 1), str(df.keys()[j])], 'LibelleIndicateurs', ('typelibelle', 'valeur'))

    for i in range(1, len(labels)):
        df[labels[0] + str(int(labels[i]))] = int(labels[i])

    if table == 'IndicateurRegion':
        importRegionIndicator(df, labels)
    elif table == 'IndicateurDepartement':
        importDptIndicator(df, labels)

    return df


def importRegionIndicator(df, labels):
    for i in range(1, len(labels)):
        addSerialKeysOfLibelle(df, [df['label' + str(i)].iloc[0], 'libelle' + str(i)])
        copyDatasetTo(df, ['num', str(labels[0] + str(int(labels[i]))), 'libelle' + str(i)], 'IndicateurRegion',
                      ('reg', 'annee', 'id_libelle'))


def importDptIndicator(df, labels):
    addDepKeysFromLibelleDf(df)

    for i in range(1, len(labels)):
        addSerialKeysOfLibelle(df, [df['label' + str(i)].iloc[0], 'libelle' + str(i)])
        copyDatasetTo(df, ['id_dep', str(labels[0] + str(int(labels[i]))), 'num', 'libelle' + str(i)],
                      'IndicateurDepartement',
                      ('id_dep', 'annee', 'DEP', 'id_libelle'))


def importSocialIndicators(filePath: str, table: str, columns):
    df, df_cols = parse_excel_sheet(filePath, 'Social')
    df1 = pd.DataFrame(df[0])
    df2 = pd.DataFrame(df[1])

    correctedKeys = {'Unnamed: 0': 'num', 'Unnamed: 1': 'libelle', 'Unnamed: 3': 'Espérance hommes 2010',
                     'Unnamed: 5': 'Espérance femmes 2010',
                     'Unnamed: 9': 'Part des jeunes 2009', 'Unnamed: 12': 'Population en zone inondable 2008'}
    df1 = df1.rename(correctedKeys, axis='columns')
    df1 = df1[~df1['num'].isin(['P', 'M', '01', '02', '03', '04', '06', 'F'])]
    df2 = df2.rename(correctedKeys, axis='columns')
    df2 = df2[~df2['num'].isin(['P', 'M', '971', '972', '973', '974', '976', 'F'])]

    importSocialLibellesFromDataset(df1, 'IndicateurRegion')
    importSocialLibellesFromDataset(df2, 'IndicateurDepartement')


# Main function to execute
if __name__ == '__main__':

    try:
        # Declaring the connection
        conn = connect(params)
        # Prepare to execute request, doing it once
        conn.set_client_encoding('UTF8')
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Delete and create tables
        print("Deleting and creating tables ...")
        cur.execute(open("delete.sql", "r").read())
        cur.execute(open("create.sql", "r").read())
        print("Succesfully deleted and created tables !")

        print("Importing datasets, please wait some seconds ...")
        # Importing part
        importCSV('../dataset/region2021.csv', ['REG', 'LIBELLE'], 'Region', ('REG', 'LIBELLE'))
        importCSV('../dataset/departement2021.csv', ['DEP', 'REG', 'LIBELLE'], 'Departement',
                  ('id_dep', 'DEP', 'REG', 'LIBELLE'), True)
        importCommune('../dataset/commune2021.csv', 'Commune', ('COM', 'DEP', 'LIBELLE'))
        importChefLieuDpt('../dataset/departement2021.csv', 'ChefLieuDepartement', ('id_com', 'id_dep'))
        importChefLieuRegion('../dataset/region2021.csv', 'ChefLieuRegion', ('id_com', 'reg'))
        importSocialIndicators('../dataset/DD-indic-reg-dep_janv2018.xls', 'IndicateurDepartement',
                               ('id_dep', 'annee', 'dep', 'id_libelle'))
        importPopulations('../dataset/base-cc-serie-historique-2018.CSV', 'IndicateurCommune',
                          ('id_com', 'annee', 'com', 'id_libelle'))

        print("Importing is done with success !")
        print("requests.py and request.sql are now usable.")

        conn.commit()  # signal the connection requests changes

    except Exception as e:
        exit("Impossible to import dataset: " + str(e))

    # Closing cursor and connection
    cur.close()
    conn.close()
