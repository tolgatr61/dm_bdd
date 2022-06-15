import psycopg2
import psycopg2.extras

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
        print('Connexion à la base de données... : ' + str(params["database"]))
        conn = psycopg2.connect(**parameters)
    except Exception as e:
        exit("Impossible to connect at database: " + str(e))
    print('Connecté à la base de données : ' + str(params["database"]))
    print('-------------------------------------------------\n')
    return conn


def requestDepartmentsOfRegion(region: str) -> list:
    query = """SELECT DEP AS num_departement, libelle FROM departement WHERE REG=%s"""
    cur.execute(query, (region,))
    res = cur.fetchall()
    return res


def printDepartmentsOfRegion(region: str, requestResult: list) -> None:
    str = "Liste de départements pour la région numéro " + region + " : \n"
    for elements in requestResult:
        str += "Département : " + elements[1] + " , avec le numéro : " + elements[0] + "\n"
    print(str)


def communesList(departement: str, minHabitants: int, year: int) -> list:
    query = """SELECT commune.COM AS numero_commune, libelle, valeur as population FROM commune join indicateurcommune i on commune.id_com = i.id_com
    join libelleindicateurs l on i.id_libelle = l.id_libelle and i.annee = %s WHERE dep = %s and valeur >= %s;"""
    cur.execute(query, (year, departement, minHabitants,))
    res = cur.fetchall()
    return res


def printCommunesList(department: str, minHabitants: int, year: int, requestResult: list) -> None:
    st = "Liste de communes avec un nombre d'habitants d'au moins : " + str(minHabitants) + ", en " + str(
        year) + " pour le département numéro " + department + " : \n"
    for elements in requestResult:
        st += "Commune : " + elements[1] + " , avec le numéro : " + elements[0] + ", avec tant d'habitants : " + str(
            elements[2]) + "\n"
    print(st)


def maxCommunes(departement: str, year: int, nbCommunes: int):
    query = """SELECT commune.com, libelle, MAX(valeur) as population FROM commune
    join indicateurcommune i on commune.id_com = i.id_com and annee = %s
    join libelleindicateurs l on l.id_libelle = i.id_libelle
    WHERE dep = %s
    GROUP BY commune.id_com
    ORDER BY population DESC LIMIT %s;
    """
    cur.execute(query, (year, departement, nbCommunes,))
    res = cur.fetchall()
    return res


def minCommunes(departement: str, year: int, nbCommunes: int):
    query = """SELECT commune.com, libelle, MIN(valeur) as population FROM commune
    join indicateurcommune i on commune.id_com = i.id_com and annee = %s
    join libelleindicateurs l on l.id_libelle = i.id_libelle
    WHERE dep = %s
    GROUP BY commune.id_com
    ORDER BY population ASC LIMIT %s;
    """
    cur.execute(query, (year, departement, nbCommunes,))
    res = cur.fetchall()
    return res


def printMaxCommunes(department: str, year: int, nbCommunes: int, requestResult: list) -> None:
    st = "Affichage de " + str(nbCommunes) + " communes en " + str(
        year) + " avec la population maximale pour le département numéro " + department + " :\n"
    for elements in requestResult:
        st += "Commune : " + elements[1] + " , avec le numéro : " + elements[0] + ", avec tant d'habitants : " + str(
            elements[2]) + "\n"
    print(st)


def printMinCommunes(department: str, year: int, nbCommunes: int, requestResult: list) -> None:
    st = "Affichage de " + str(nbCommunes) + " communes en " + str(
        year) + " avec la population minimale pour le département numéro " + department + " :\n"
    for elements in requestResult:
        st += "Commune : " + elements[1] + " , avec le numéro : " + elements[0] + ", avec tant d'habitants : " + str(
            elements[2]) + "\n"
    print(st)


if __name__ == '__main__':
    try:
        # Declaring the connection
        conn = connect(params)
        # Prepare to execute request, doing it once
        conn.set_client_encoding('UTF8')
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        regionNumber = '28'
        printDepartmentsOfRegion(regionNumber, requestDepartmentsOfRegion(regionNumber))

        depNumber = '01'
        minHabitants = 10000
        year = 2018
        printCommunesList(depNumber, minHabitants, year, communesList(depNumber, minHabitants, year))
        nbCommunes = 3
        printMaxCommunes(depNumber, year, nbCommunes, maxCommunes(depNumber, year, nbCommunes))
        printMinCommunes(depNumber, year, nbCommunes, minCommunes(depNumber, year, nbCommunes))

        conn.commit()  # signal the connection requests changes

    except Exception as e:
        exit("Impossible to import dataset: " + str(e))

    cur.close()
    conn.close()
