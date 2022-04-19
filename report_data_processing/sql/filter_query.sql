WITH
    target_dois AS (
    SELECT doi as d,
        affs.identifier,
        affs.name,
        affs.country,
        affs.region,
        crossref.published_year,
        crossref.title,
        mag.abstract,
        unpaywall.is_oa
    FROM
        `{doi_table}`, UNNEST(affiliations.institutions) as affs
    WHERE
        crossref.published_year > 2014 AND
        crossref.published_year < 2023
)

SELECT
    *
FROM target_dois as t INNER JOIN `{truth_table}` as s
    on d = doi