-- openalex_openaire_doi_match_2020_2023
create or replace table cwts-leiden.projectdb_openalex_openaire_2025.openalex_openaire_doi_match_2020_2023
as
(
  with openaire_pub as (
    select id as openaire_pub_id, json_value(pub_pid,'$.value') as doi
    from openaire-graph.oag_2025_01.publications,
    unnest(json_query_array(pids)) as pub_pid
    where json_value(pub_pid,'$.scheme') = 'doi'
  )
  select a.work_id as openalex_work_id, b.openaire_pub_id, lower(a.doi) as doi
  from cwts-leiden.openalex_2024aug.work as a
  join openaire_pub as b on lower(a.doi) = lower(b.doi)
  where a.pub_year between 2020 and 2023
);

-- openaire_pub_lu_2020_2023
create or replace table cwts-leiden.projectdb_openalex_openaire_2025.openaire_pub_lu_2020_2023
as
(
  with openaire_org as (
    select id as openaire_org_id, json_value(org_pid,'$.value') as ror_id
    from openaire-graph.oag_2025_01.organizations,
    unnest(json_query_array(pids)) as org_pid
    where json_value(org_pid,'$.scheme') = 'ROR'
  )
  select distinct a.openaire_pub_id
  from
    cwts-leiden.projectdb_openalex_openaire_2025.openalex_openaire_doi_match_2020_2023 as a,
    openaire-graph.oag_2025_01.relations as b,
    openaire_org as c
  where a.openaire_pub_id = b.source
    and b.target = c.openaire_org_id
    and b.relationName = 'hasAuthorInstitution'
    and c.ror_id in ('https://ror.org/027bh9e22', 'https://ror.org/05xvt9f17', 'https://ror.org/03es66g06')
);

-- openalex_pub_lu_2020_2023
create or replace table cwts-leiden.projectdb_openalex_openaire_2025.openalex_pub_lu_2020_2023
as
(
  select distinct a.openalex_work_id
  from cwts-leiden.projectdb_openalex_openaire_2025.openalex_openaire_doi_match_2020_2023 as a
  join cwts-leiden.openalex_2024aug.work_affiliation_institution as b on a.openalex_work_id = b.work_id
  join cwts-leiden.openalex_2024aug.institution as c on b.institution_id = c.institution_id
  where c.ror_id in ('027bh9e22', '05xvt9f17', '03es66g06')
);

-- openalex_doi_lu_2020_2023
create or replace table cwts-leiden.projectdb_openalex_openaire_2025.openalex_doi_lu_2020_2023
as
(
  select distinct a.doi
  from cwts-leiden.projectdb_openalex_openaire_2025.openalex_openaire_doi_match_2020_2023 as a
  join cwts-leiden.projectdb_openalex_openaire_2025.openalex_pub_lu_2020_2023 as b on a.openalex_work_id = b.openalex_work_id
);

-- openaire_doi_lu_2020_2023
create or replace table cwts-leiden.projectdb_openalex_openaire_2025.openaire_doi_lu_2020_2023
as
(
  select distinct a.doi
  from cwts-leiden.projectdb_openalex_openaire_2025.openalex_openaire_doi_match_2020_2023 as a
  join cwts-leiden.projectdb_openalex_openaire_2025.openaire_pub_lu_2020_2023 as b on a.openaire_pub_id = b.openaire_pub_id
);

-- doi_lu_2020_2023_openalex_and_openaire
create or replace table cwts-leiden.projectdb_openalex_openaire_2025.doi_lu_2020_2023_openalex_and_openaire
as
(
  select a.doi, row_number() over (order by rand()) as random_order
  from cwts-leiden.projectdb_openalex_openaire_2025.openalex_doi_lu_2020_2023 as a
  join cwts-leiden.projectdb_openalex_openaire_2025.openaire_doi_lu_2020_2023 as b on a.doi = b.doi
);

-- doi_lu_2020_2023_openalex_only
create or replace table cwts-leiden.projectdb_openalex_openaire_2025.doi_lu_2020_2023_openalex_only
as
(
  select a.doi, row_number() over (order by rand()) as random_order
  from cwts-leiden.projectdb_openalex_openaire_2025.openalex_doi_lu_2020_2023 as a
  left join cwts-leiden.projectdb_openalex_openaire_2025.openaire_doi_lu_2020_2023 as b on a.doi = b.doi
  where b.doi is null
);

-- doi_lu_2020_2023_openaire_only
create or replace table cwts-leiden.projectdb_openalex_openaire_2025.doi_lu_2020_2023_openaire_only
as
(
  select a.doi, row_number() over (order by rand()) as random_order
  from cwts-leiden.projectdb_openalex_openaire_2025.openaire_doi_lu_2020_2023 as a
  left join cwts-leiden.projectdb_openalex_openaire_2025.openalex_doi_lu_2020_2023 as b on a.doi = b.doi
  where b.doi is null
);

select count(*) from cwts-leiden.projectdb_openalex_openaire_2025.doi_lu_2020_2023_openalex_and_openaire;
--30374

select count(*) from cwts-leiden.projectdb_openalex_openaire_2025.doi_lu_2020_2023_openalex_only;
--4548

select count(*) from cwts-leiden.projectdb_openalex_openaire_2025.doi_lu_2020_2023_openaire_only;
--7293

-- doi_lu_2020_2023_openalex_and_openaire_sample
create or replace table cwts-leiden.projectdb_openalex_openaire_2025.doi_lu_2020_2023_openalex_and_openaire_sample
as
(
  select b.random_order, a.*
  from cwts-leiden.projectdb_openalex_openaire_2025.openalex_openaire_doi_match_2020_2023 as a
  join cwts-leiden.projectdb_openalex_openaire_2025.doi_lu_2020_2023_openalex_and_openaire as b on a.doi = b.doi
  order by random_order
);

-- doi_lu_2020_2023_openalex_only_sample
create or replace table cwts-leiden.projectdb_openalex_openaire_2025.doi_lu_2020_2023_openalex_only_sample
as
(
  select b.random_order, a.*
  from cwts-leiden.projectdb_openalex_openaire_2025.openalex_openaire_doi_match_2020_2023 as a
  join cwts-leiden.projectdb_openalex_openaire_2025.doi_lu_2020_2023_openalex_only as b on a.doi = b.doi
  order by random_order
);

-- doi_lu_2020_2023_openaire_only_sample
create or replace table cwts-leiden.projectdb_openalex_openaire_2025.doi_lu_2020_2023_openaire_only_sample
as
(
  select b.random_order, a.*
  from cwts-leiden.projectdb_openalex_openaire_2025.openalex_openaire_doi_match_2020_2023 as a
  join cwts-leiden.projectdb_openalex_openaire_2025.doi_lu_2020_2023_openaire_only as b on a.doi = b.doi
  order by random_order
);
