/*
 Request files
*/

/*
 Import a psql dependency for crosstab() function
 */
CREATE EXTENSION IF NOT EXISTS tablefunc;

/* Q2 */

DROP VIEW IF EXISTS DepartementPopulations CASCADE;
CREATE OR REPLACE VIEW DepartementPopulations AS
SELECT d.id_dep, i.annee, SUM(valeur) as population
FROM commune
         join departement d on d.dep = commune.dep
         join indicateurcommune i on commune.id_com = i.id_com
         join libelleindicateurs l on i.id_libelle = l.id_libelle
WHERE typelibelle IN ('P18_POP', 'P13_POP', 'P08_POP', 'D99_POP', 'D90_POP', 'D82_POP', 'D75_POP', 'D68_POP')
GROUP BY d.id_dep, i.annee
ORDER BY d.id_dep, i.annee;

DROP VIEW IF EXISTS RegionPopulations CASCADE;
CREATE OR REPLACE VIEW RegionPopulations AS
SELECT reg, annee, SUM(population) as population
FROM DepartementPopulations
         join departement d on DepartementPopulations.id_dep = d.id_dep
WHERE reg = d.reg
GROUP BY reg, annee
ORDER BY reg, annee;

DROP VIEW IF EXISTS IndicateursDepartements;
CREATE OR REPLACE VIEW IndicateursDepartements AS
SELECT d.id_dep, i.annee, i.id_libelle, SUM(valeur) as statistic
FROM commune
         join departement d on d.dep = commune.dep
         join indicateurdepartement i on d.id_dep = i.id_dep
         join libelleindicateurs l on i.id_libelle = l.id_libelle
WHERE typelibelle NOT IN ('P18_POP', 'P13_POP', 'P08_POP', 'D99_POP', 'D90_POP', 'D82_POP', 'D75_POP', 'D68_POP')
GROUP BY d.id_dep, annee, i.id_libelle
ORDER BY d.id_dep, annee;

DROP VIEW IF EXISTS IndicateursRegions;
CREATE OR REPLACE VIEW IndicateursRegions AS
SELECT d.id_dep, i.annee, i.id_libelle, SUM(valeur) as statistic
FROM commune
         join departement d on d.dep = commune.dep
         join indicateurregion i on d.reg = i.reg
         join libelleindicateurs l on l.id_libelle = i.id_libelle
WHERE typelibelle NOT IN ('P18_POP', 'P13_POP', 'P08_POP', 'D99_POP', 'D90_POP', 'D82_POP', 'D75_POP', 'D68_POP')
GROUP BY d.id_dep, annee, i.id_libelle
ORDER BY d.id_dep, annee;

/* Q3 && used for 4/5 too */

DROP VIEW IF EXISTS anotherDptPopFormat;
CREATE OR REPLACE VIEW anotherDptPopFormat AS
SELECT *
FROM crosstab(
             'SELECT id_dep, annee, population
              FROM   DepartementPopulations
              ORDER  BY 1,2'
         ) AS population ("id_dep" int, "1968" double precision, "1975" double precision, "1982" double precision,
                          "1990" double precision,
                          "1999" double precision, "2008" double precision, "2013" double precision,
                          "2018" double precision);

DROP VIEW IF EXISTS anotherRegionPopFormat;
CREATE OR REPLACE VIEW anotherRegionPopFormat AS
SELECT *
FROM crosstab(
             'SELECT reg, annee, population
              FROM   RegionPopulations
              ORDER  BY 1,2'
         ) AS population ("reg" numeric(2), "1968" double precision, "1975" double precision, "1982" double precision,
                          "1990" double precision, "1999" double precision, "2008" double precision,
                          "2013" double precision, "2018" double precision);

create or replace procedure writeDptsPop()
    language plpgsql
as
$$
begin
    if NOT exists(select constraint_name
                  from information_schema.table_constraints
                  where table_name = 'dptspopulations'
                    and constraint_type = 'PRIMARY KEY') then
        CREATE TABLE IF NOT EXISTS DptsPopulations AS
        SELECT * FROM anotherDptPopFormat;
        ALTER TABLE DptsPopulations
            ADD COLUMN IF NOT EXISTS id_pop SERIAL
                constraint pk_DPop PRIMARY KEY;
        ALTER TABLE DptsPopulations
            ADD CONSTRAINT fk_depIdPop FOREIGN KEY (id_dep) REFERENCES departement (id_dep);
        ALTER TABLE departement
            ADD COLUMN IF NOT EXISTS id_pop int
                constraint fkIdDPop REFERENCES DptsPopulations (id_pop);
    else
        UPDATE DptsPopulations as dt
        SET ("1968", "1975", "1982", "1990", "1999", "2008", "2013", "2018")
                = (d."1968", d."1975", d."1982", d."1990", d."1999", d."2008", d."2013", d."2018")
        FROM anotherDptPopFormat as d
        WHERE d.id_dep = dt.id_dep;

    end if;
end;
$$;

create or replace procedure writeRegPops()
    language plpgsql
as
$$
begin
    DROP TABLE IF EXISTS regpopulations CASCADE;
    CREATE TABLE IF NOT EXISTS RegPopulations AS
    SELECT * FROM anotherRegionPopFormat;
    if NOT exists(select constraint_name
                  from information_schema.table_constraints
                  where table_name = 'regpopulations'
                    and constraint_type = 'PRIMARY KEY') then
        ALTER TABLE RegPopulations
            ADD COLUMN IF NOT EXISTS id_pop SERIAL
                constraint pk_RPop PRIMARY KEY;
        ALTER TABLE RegPopulations
            ADD CONSTRAINT fk_regPop FOREIGN KEY (reg) REFERENCES region (reg);
        ALTER TABLE region
            ADD COLUMN IF NOT EXISTS id_pop int
                constraint fkRPop REFERENCES regpopulations (id_pop);
    else
        UPDATE RegPopulations as rt
        SET ("1968", "1975", "1982", "1990", "1999", "2008", "2013", "2018")
                = (r."1968", r."1975", r."1982", r."1990", r."1999", r."2008", r."2013", r."2018")
        FROM anotherRegionPopFormat as r
        WHERE r.reg = rt.reg;
    end if;
end;
$$;

create or replace procedure writePopulations()
    language plpgsql
as
$$
DECLARE
    TABLE_RECORD RECORD;
begin

    call writeDptsPop();
    call writeRegPops();

    FOR TABLE_RECORD IN SELECT * FROM dptspopulations
        LOOP
            UPDATE departement
            SET id_pop = TABLE_RECORD.id_pop
            WHERE TABLE_RECORD.id_dep = id_dep;
        end loop;

    FOR TABLE_RECORD IN SELECT * FROM regpopulations
        LOOP
            UPDATE region
            SET id_pop = TABLE_RECORD.id_pop
            WHERE TABLE_RECORD.reg = reg;
        end loop;

end;
$$;

/*
 There is a need to run this once to create the necessary tables,
 careful when you will have the triggers it will be restricted to be launched and an error will occur.
 So I added a line to remove the triggers to allow all the execution of the file n times.
 */
ALTER TABLE region DISABLE TRIGGER ALL;
ALTER TABLE departement DISABLE TRIGGER ALL;
CALL writePopulations();

/* Q4 && 5 */

/*
  Read only trigger will raise an exception to make the request as an error.
 */
create or replace function readOnly()
    returns trigger
as
$$
begin
    raise exception 'This table is unmodifiable';
end;
$$
    language plpgsql;

/*
 Creating the triggers
 (and deleting them before if it already exists in the database, to assure multiple executions of the file,
 otherwise the triggers will simply return an error on the previous requests with a new execution)
 */
DROP TRIGGER IF EXISTS restrictDpt on departement;
DROP TRIGGER IF EXISTS restrictReg on region;

create trigger restrictDpt
    before insert or update or delete
    on departement
    for each row
execute procedure readOnly();

create trigger restrictReg
    before insert or update or delete
    on region
    for each row
execute procedure readOnly();

/* Testing the writing, deleting over the table
update region set id_pop = 4
where reg = 1;

update departement set id_pop = 4
where reg = 1;

insert into departement(dep, reg, libelle, id_pop)  VALUES (101,'test', 4);
insert into region(reg, libelle, id_pop) VALUES (120,'test', 4);

delete from departement WHERE id_dep=1;
delete from region WHERE reg = 1;

   Returns
   -> [P0001] ERREUR: This table is unmodifiable
   -> Où  : fonction PL/pgsql readonly(), ligne 3 à RAISE
*/


create or replace function updatePopulations()
    returns trigger
as
$$
begin
    ALTER TABLE region
        DISABLE TRIGGER restrictreg;
    ALTER TABLE departement
        DISABLE TRIGGER restrictdpt;
    if exists(SELECT i.id_libelle, c.id_com, d.id_dep, r.reg
              FROM libelleindicateurs
                       join indicateurcommune i on libelleindicateurs.id_libelle = i.id_libelle
                       join commune c on i.id_com = c.id_com
                       join departement d on c.dep = d.dep
                       join region r on d.reg = r.reg
              WHERE i.id_libelle = OLD.id_libelle
                and r.id_pop is not NULL
                and d.id_pop is not NULL
                and typelibelle in
                    ('P18_POP', 'P13_POP', 'P08_POP', 'D99_POP', 'D90_POP', 'D82_POP', 'D75_POP', 'D68_POP')) then
        call writePopulations();
    end if;
    ALTER TABLE region
        ENABLE TRIGGER restrictreg;
    ALTER TABLE departement
        ENABLE TRIGGER restrictdpt;
    return null;
end;
$$
    language plpgsql;

DROP TRIGGER IF EXISTS updatePops on libelleindicateurs;

CREATE TRIGGER updatePops
    AFTER INSERT or UPDATE OF valeur
    on libelleindicateurs
    FOR EACH ROW
EXECUTE PROCEDURE updatePopulations();

/**
  A test
UPDATE libelleindicateurs
SET valeur = 20000000
WHERE id_libelle = 1200;

SELECT id_libelle, valeur
FROM libelleindicateurs
where id_libelle = 1200;

 */

/***
  Triggers to make our populations updatable at insertion too, and when a new indicator is inserted.
 */

DROP TRIGGER IF EXISTS updatePopulationIndicator on indicateurcommune;
CREATE TRIGGER updatePopulationIndicator
    AFTER INSERT
    ON indicateurcommune
    FOR EACH ROW
EXECUTE PROCEDURE updatePopulations();

DROP TRIGGER IF EXISTS updatePopulationLibelle on libelleindicateurs;
CREATE TRIGGER updatePopulationLibelle
    AFTER INSERT
    ON libelleindicateurs
    FOR EACH ROW
EXECUTE PROCEDURE updatePopulations();

/*
 Testing with a new random indicator linked to a population label

INSERT INTO libelleindicateurs(typelibelle, valeur)
VALUES ('P08_POP', 100000000000);
INSERT INTO indicateurcommune(id_com, annee, com, id_libelle)
VALUES (501, 2013, 97225, (SELECT id_libelle from libelleindicateurs WHERE valeur = 100000000000));

 */

/* Q6 */

EXPLAIN ANALYZE
SELECT *
FROM indicateurcommune;

EXPLAIN ANALYZE
SELECT *
FROM indicateurcommune
         join libelleindicateurs l on l.id_libelle = indicateurcommune.id_libelle;

EXPLAIN ANALYZE
SELECT *
from region
         join regpopulations r on r.id_pop = region.id_pop
ORDER BY r."2018";

EXPLAIN ANALYZE
SELECT *
from region
         join regpopulations r on r.id_pop = region.id_pop
ORDER BY r."2018"
limit 1;

EXPLAIN ANALYZE SELECT * from region ORDER BY reg;

EXPLAIN ANALYZE SELECT * from departement WHERE id_dep = 01;

EXPLAIN ANALYZE SELECT * from departement WHERE dep = '01';

EXPLAIN ANALYZE SELECT * FROM commune;

EXPLAIN ANALYZE SELECT id_com FROM commune where id_com = 120;

EXPLAIN ANALYZE SELECT com FROM commune WHERE dep = '36';

/* Q7 */

/* Less than <=5000 population on a commune indicator */

EXPLAIN ANALYZE SELECT com FROM commune WHERE dep = '36';
DROP INDEX IF EXISTS idx_dep;
CREATE INDEX idx_dep ON commune (dep);
EXPLAIN ANALYZE SELECT com FROM commune WHERE dep = '36';

EXPLAIN ANALYZE
SELECT valeur as population
FROM commune
         join indicateurcommune i on commune.id_com = i.id_com
         join libelleindicateurs l on i.id_libelle = l.id_libelle
         WHERE valeur <= 5000;

DROP INDEX IF EXISTS idx_val;
CREATE INDEX idx_val ON libelleindicateurs (valeur);

EXPLAIN ANALYZE
SELECT valeur
FROM commune
         join indicateurcommune i on commune.id_com = i.id_com
         join libelleindicateurs l on i.id_libelle = l.id_libelle
         WHERE valeur <= 5000;


EXPLAIN ANALYZE
SELECT "2018"
from regpopulations;

EXPLAIN ANALYZE
SELECT *
from region
         join regpopulations r on r.id_pop = region.id_pop
ORDER BY r."2018";

DROP INDEX IF EXISTS idx_reg2018;
CREATE INDEX idx_reg2018 ON regpopulations ("2018");

EXPLAIN ANALYZE
SELECT "2018"
from regpopulations;

EXPLAIN ANALYZE
SELECT *
from region
         join regpopulations r on r.id_pop = region.id_pop
ORDER BY r."2018";

EXPLAIN ANALYZE
SELECT *
from region
         join regpopulations r on r.id_pop = region.id_pop
ORDER BY r."2018" limit 1;



/* Taken from https://stackoverflow.com/questions/970562/postgres-and-indexes-on-foreign-keys-and-primary-keys
   It shows that our keys are also indexes with psql specific tables.
   */
select n.nspname as "Schema"
     , t.relname as "Table"
     , c.relname as "Index"
from pg_catalog.pg_class c
         join pg_catalog.pg_namespace n on n.oid = c.relnamespace
         join pg_catalog.pg_index i on i.indexrelid = c.oid
         join pg_catalog.pg_class t on i.indrelid = t.oid
where c.relkind = 'i'
  and n.nspname not in ('pg_catalog', 'pg_toast')
  and pg_catalog.pg_table_is_visible(c.oid)
order by n.nspname
       , t.relname
       , c.relname


/* Q8 */

/* Si besoin de tester, c'est à décommenter et à utiliser en parallèle avec un autre client éventuellement
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
INSERT INTO commune(com, dep, libelle)
VALUES('52000', '01', 'test');
COMMIT;

BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
    UPDATE regpopulations SET "1975" = -1000 WHERE id_pop = 2;
    DELETE from regpopulations WHERE id_pop = 10;
COMMIT;

BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
    UPDATE regpopulations SET "1975" = -1000 WHERE id_pop = 2;
COMMIT;
 */


/*  Some trials or unused requests

SELECT commune.id_com, commune.com, libelle, dep, MIN(valeur) as population FROM commune
    join indicateurcommune i on commune.id_com = i.id_com and annee = 2018
    join libelleindicateurs l on l.id_libelle = i.id_libelle
    WHERE dep = '01'
    GROUP BY commune.id_com
    ORDER BY population ASC LIMIT 1;

SELECT commune.id_com, commune.com, libelle, dep, MAX(valeur) as population FROM commune
    join indicateurcommune i on commune.id_com = i.id_com and annee = 2018
    join libelleindicateurs l on l.id_libelle = i.id_libelle
    WHERE dep = '01'
    GROUP BY commune.id_com
    ORDER BY population DESC LIMIT 1;

SELECT DEP AS num_departement, libelle FROM departement WHERE REG='28';

SELECT commune.COM AS numero_commune, libelle, valeur as population FROM commune join indicateurcommune i on commune.id_com = i.id_com
    join libelleindicateurs l on i.id_libelle = l.id_libelle and i.annee = 2018 WHERE dep = '01' and valeur >= 10000;


SELECT commune.libelle, r.libelle FROM commune join departement d on commune.dep = d.dep join region r on r.reg = d.reg

SELECT r.reg, sum(valeur) as population2018 FROM commune join departement d on commune.dep = d.dep
    join region r on r.reg = d.reg
    join indicateurcommune i on commune.id_com = i.id_com and commune.com = i.com join libelleindicateurs l on i.id_libelle = l.id_libelle
    WHERE typelibelle = 'P18_POP'
                GROUP BY r.reg;

SELECT reg, (SELECT sum(valeur) as population2018 FROM commune join departement d on commune.dep = d.dep
    join region r on r.reg = d.reg
    join indicateurcommune i on commune.id_com = i.id_com and commune.com = i.com join libelleindicateurs l on i.id_libelle = l.id_libelle
    WHERE typelibelle = 'P18_POP'
                GROUP BY r.reg
                ORDER BY sum(valeur) LIMIT 2)
FROM region;
*/