create table Region (
    REG numeric(2) constraint reg_positive check (REG > 0) PRIMARY KEY,
    LIBELLE varchar(30) not null unique
);

create table Departement (
    id_dep SERIAL PRIMARY KEY,
    DEP char(3) not null unique,
    REG numeric(2) REFERENCES Region(REG),
    LIBELLE varchar(30) not null unique
);

create table Commune (
    id_com SERIAL PRIMARY KEY,
    COM char(5) not null unique,
    DEP char(3) REFERENCES Departement(DEP),
    LIBELLE varchar(200) not null
);

create table ChefLieuRegion (
    id_com int REFERENCES Commune(id_com),
    REG numeric(2) REFERENCES Region(REG),
    constraint pkey_chefLieuRegion PRIMARY KEY (id_com, REG)
);

create table ChefLieuDepartement (
    id_com int REFERENCES Commune(id_com),
    id_dep int REFERENCES Departement(id_dep),
    constraint pkey_chefLieuDepartement PRIMARY KEY (id_com, id_dep)
);

create table LibelleIndicateurs (
    id_libelle SERIAL PRIMARY KEY,
    typeLibelle varchar(200) not null,
    valeur double precision not null
);

create table IndicateurRegion (
    REG numeric(2) REFERENCES Region(REG),
    Annee smallint,
    id_libelle int not null REFERENCES LibelleIndicateurs(id_libelle),
    constraint pKey_IndicateurRegion PRIMARY KEY (REG, Annee, id_libelle)
);

create table IndicateurCommune (
    id_com int references Commune(id_com),
    Annee smallint,
    COM char(5) REFERENCES Commune(COM),
    id_libelle int not null REFERENCES LibelleIndicateurs(id_libelle),
    constraint pKey_IndicateurCommune PRIMARY KEY (id_com, Annee, id_libelle)
);

create table IndicateurDepartement (
    id_dep int references Departement(id_dep),
    Annee smallint,
    DEP char(3) REFERENCES Departement(DEP),
    id_libelle int not null REFERENCES LibelleIndicateurs(id_libelle),
    constraint pKey_IndicateurDepartement PRIMARY KEY (id_dep, Annee, id_libelle)
);


/* clé étrangère sur une clé primaire ou unique, puis on se resert des clés pour remplir les autres tables */