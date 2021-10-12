WITH
    target_dois AS (
    SELECT doi as d,
        affs.identifier,
        affs.name,
        crossref.published_year,
        crossref.title,
        mag.abstract,
        unpaywall.is_oa
    FROM
        `academic-observatory.observatory.doi20210821`, UNNEST(affiliations.institutions) as affs
    WHERE
        crossref.published_year > 2014 AND
        crossref.published_year < 2023 AND
        affs.identifier IN ("grid.1032.0",
                            "grid.1001.0", -- Go8
                            "grid.1002.3",
                            "grid.1003.2",
                            "grid.1005.4",
                            "grid.1008.9",
                            "grid.1010.0",
                            "grid.1012.2",
                            "grid.1013.3",
                            "grid.1017.7", -- ATN
                            "grid.1026.5",
                            "grid.1021.2",
                            "grid.117476.2",
                            "grid.4991.5", -- Oxford
                            "grid.20861.3d", -- CalTech
                            "grid.38142.3c", -- Harvard
                            "grid.168010.e", -- Stanford
                            "grid.5335.0", --Cambridge
                            "grid.116068.8", -- MIT,
                            "grid.16750.35", -- Princeton
                            "grid.47840.3f", -- UC Berkeley
                            "grid.47100.32", -- Yale
                            "grid.170205.1" -- Chicago
                            )
)

SELECT
    *
FROM target_dois as t INNER JOIN `coki-scratch-space.curtin.doi_sdgs` as s
    on d = doi