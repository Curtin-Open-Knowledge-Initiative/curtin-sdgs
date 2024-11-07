WITH
    target_dois AS (
    SELECT doi as d,
        affs.identifier as identifier,
        affs.name,
        affs.country,
        affs.region,
        crossref.published_year,
        crossref.title,
        crossref.abstract,
        unpaywall.is_oa,
        openalex.authorships
    FROM
        `{doi_table}`, UNNEST(affiliations.institutions) as affs
    WHERE
        crossref.published_year > 2015 AND
        crossref.published_year < 2024
)

SELECT
    *
FROM target_dois as t INNER JOIN `{truth_table}` as s
    on d = doi