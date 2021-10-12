WITH titleabs AS (
    SELECT doi,
           CONCAT( " ",
                   IF(ARRAY_LENGTH(crossref.title) > 0,
                      LOWER(crossref.title[OFFSET(0)]), "")
               , " ", LOWER(mag.abstract)) as title_abs,
           ARRAY_TO_STRING(
                   ARRAY_CONCAT(
                           ARRAY(SELECT " "),
                           ARRAY((SELECT LOWER(DisplayName) from UNNEST(mag.fields.level_0))),
                           ARRAY((SELECT LOWER(DisplayName) from UNNEST(mag.fields.level_1))),
                           ARRAY((SELECT LOWER(DisplayName) from UNNEST(mag.fields.level_2))),
                           ARRAY((SELECT LOWER(DisplayName) from UNNEST(mag.fields.level_3))),
                           ARRAY((SELECT LOWER(DisplayName) from UNNEST(mag.fields.level_4))),
                           ARRAY((SELECT LOWER(DisplayName) from UNNEST(mag.fields.level_5)))
                       ), " "
               )                           as fields
    FROM `academic-observatory.observatory.doi20210821`
)

SELECT doi,
       CASE
           WHEN
                       (title_abs LIKE "% poverty%" or fields LIKE "% poverty%")
                       AND
                       (title_abs NOT LIKE "% health%" AND fields NOT LIKE "% health%" AND
                        title_abs NOT LIKE "% disease%" AND fields NOT LIKE "% disease%" AND
                        title_abs NOT LIKE "% infection%" AND fields NOT LIKE "% infection%")
                   OR
                       (title_abs LIKE "% socio%economic%" OR fields LIKE "% socio%economic%" OR
                        title_abs LIKE "% resilience%" OR fields LIKE "% resilience%"
                           OR title_abs LIKE "% income distribution%" OR fields LIKE "% income distribution%" OR
                        title_abs LIKE "% wealth distribution%" or fields LIKE "% wealth distribution%"
                           OR title_abs LIKE "% disadvantage%" OR fields LIKE "% disadvantage%")
               THEN TRUE
           WHEN
               (title_abs LIKE "% social protection%" OR fields LIKE "% social protection%" OR
                title_abs LIKE "% extreme poverty%" OR fields LIKE "% extreme poverty%"
                   OR title_abs LIKE "% entrenched deprivation%" OR fields LIKE "% entrenched deprivation%" OR
                title_abs LIKE "% poverty eradication%" OR fields LIKE "% poverty eradication%"
                   OR title_abs LIKE "% poverty line%" OR fields LIKE "% poverty line%")
               THEN TRUE
           ELSE FALSE END AS sdg_1_no_poverty,
       CASE
           WHEN
               (title_abs LIKE "% crop diversity%" OR fields LIKE "% crop diversity%" OR
                title_abs LIKE "% food produc%" OR
                fields LIKE "% food produc%" or title_abs LIKE "% agricult% produc%" or
                fields LIKE "% agricult% produc%")
               THEN TRUE
           WHEN
                   (title_abs LIKE "% security%" OR title_abs LIKE "% reserves%" OR title_abs LIKE "% resilien%" OR
                    title_abs LIKE "% sustainab%")
                   AND
                   (title_abs LIKE "% farming%" OR title_abs LIKE "% food%" OR title_abs LIKE "% hunger%" OR
                    title_abs LIKE "% hungry%" or title_abs LIKE "% agricultur%" OR title_abs LIKE "% nutrition%" OR
                    title_abs LIKE "% nourish%")
               THEN TRUE
           WHEN
               (fields LIKE "% food security%" OR fields LIKE "% food insecurity%" OR fields LIKE "% food reserves%")
               THEN TRUE
           ELSE FALSE END AS sdg_2_zero_hunger,
       CASE
           WHEN
                   (title_abs LIKE "% access%" OR title_abs LIKE "% afford%" OR title_abs LIKE "% finance%" OR
                    title_abs LIKE "% poverty%" OR title_abs LIKE "% low income%" OR
                    title_abs LIKE "% middle income%" OR
                    title_abs LIKE "% wellbeing%" or title_abs LIKE "% well-being%")
                   AND
                   (title_abs LIKE "% health%" OR title_abs LIKE "% vaccin%" or title_abs LIKE "% medicin%" or
                    title_abs LIKE "% malaria" or title_abs LIKE "% HIV%" OR title_abs LIKE "% tuberculosis%" OR
                    title_abs LIKE "% hepatitis%" or title_abs LIKE "% immuni%")
               THEN TRUE
           WHEN
                   (title_abs LIKE "% mortality%" or fields LIKE "% mortality%")
                   AND
                   (title_abs LIKE "% neonatal%" or title_abs LIKE "% maternal%" or title_abs LIKE "% child%" OR
                    title_abs LIKE "% premature%" OR title_abs LIKE "% rate%" OR title_abs LIKE "% suicide%"
                       OR title_abs LIKE "% cancer%" OR title_abs LIKE "% cardiovascular%" or
                    title_abs LIKE "% heart disease%" or title_abs LIKE "% diabetes%" or
                    title_abs LIKE "% chronic respiratory disease%"
                       OR fields LIKE "% neonatal%" or fields LIKE "% maternal%" or fields LIKE "% child%" OR
                    fields LIKE "% premature%" OR fields LIKE "% rate%" OR fields LIKE "% suicide%"
                       OR fields LIKE "% cancer%" OR fields LIKE "% cardiovascular%" or
                    fields LIKE "% heart disease%" or
                    fields LIKE "% diabetes%" or fields LIKE "% chronic respiratory disease%")
               THEN TRUE
           WHEN
               (title_abs LIKE "% life expectancy%" or title_abs LIKE "% universal health%" or
                title_abs LIKE "% world health organi%" or title_abs LIKE "% major disease%" or
                title_abs LIKE "% tropical disease%" or title_abs LIKE "% communicable disease%"
                   OR fields LIKE "% life expectancy%" or fields LIKE "% universal health%" or
                fields LIKE "% world health organi%" or fields LIKE "% major disease%" or
                fields LIKE "% tropical disease%" or fields LIKE "% communicable disease%")
               THEN TRUE
           ELSE FALSE END AS sdg_3_health_well_being,
       CASE
           WHEN
                   (title_abs LIKE "% educat%" OR title_abs LIKE "% numeracy%" OR title_abs LIKE "% literacy%" OR
                    title_abs LIKE "% illiterate%" OR title_abs LIKE "% pedagog%" OR title_abs LIKE "% schooling%")
                   AND
                   (title_abs LIKE "% inclusi%" OR title_abs LIKE "% equality%" OR title_abs LIKE "% equit%" OR
                    title_abs LIKE "% primary school%" OR title_abs LIKE "% quality%" OR
                    title_abs LIKE "% marginali%" OR
                    title_abs LIKE "% participation%" OR title_abs LIKE "% childhood%")
               THEN TRUE
           WHEN
                   (fields LIKE "% educat%" OR fields LIKE "% numeracy%" OR fields LIKE "% literacy%" OR
                    fields LIKE "% illiterate%" OR fields LIKE "% pedagog%" OR fields LIKE "% schooling%")
                   AND
                   (fields LIKE "% inclusi%" OR fields LIKE "% equality%" OR fields LIKE "% equit%" OR
                    fields LIKE "% primary school%" OR fields LIKE "% quality%" OR fields LIKE "% marginali%" OR
                    fields LIKE "% participation%" OR fields LIKE "% childhoos%")
               THEN TRUE
           WHEN
               (title_abs LIKE "% lifelong learning%" OR title_abs LIKE "% life-long learning%" OR
                title_abs LIKE "% proficiency standards%" OR title_abs LIKE "% early childhood development%" OR
                fields LIKE "% lifelong learning%" OR fields LIKE "% life-long learning%" OR
                fields LIKE "% proficiency standards%" OR fields LIKE "% early childhood development%")
               THEN TRUE
           ELSE FALSE END AS sdg_4_quality_education,
       CASE
           WHEN
                   (title_abs LIKE "% gender%" OR title_abs LIKE "% woman%" OR title_abs LIKE "% women%" OR
                    title_abs LIKE "% girld%" OR title_abs LIKE "% female%" OR
                    fields LIKE "% gender%" OR fields LIKE "% woman%" OR fields LIKE "% women%" OR
                    fields LIKE "% girld%" OR
                    fields LIKE "% female%")
                   AND
                   (title_abs LIKE "% equality%" OR title_abs LIKE "% equit%" OR title_abs LIKE "% marginali%" OR
                    title_abs LIKE "% parity%" OR title_abs LIKE "% pay%" OR title_abs LIKE "% salary%"
                       OR title_abs LIKE "% salaries%" OR title_abs LIKE "% discriminat%" OR
                    title_abs LIKE "% exploit%" OR
                    title_abs LIKE "% empower%" OR title_abs LIKE "% disadvantag%" OR title_abs LIKE "% dignity%"
                       OR title_abs LIKE "% violence%" OR title_abs LIKE "% labor force%" OR
                    title_abs LIKE "% labour force%" OR title_abs LIKE "% work force%" OR
                    title_abs LIKE "% leadership%" OR
                    title_abs LIKE "% managerial%"
                       OR title_abs LIKE "% underrepresent%" OR title_abs LIKE "% under represent%" OR
                    title_abs LIKE "% representation%" OR title_abs LIKE "% diversity%" OR
                    title_abs LIKE "% domestic violence%"
                       OR fields LIKE "% equality%" OR fields LIKE "% equit%" OR fields LIKE "% marginali%" OR
                    fields LIKE "% parity%" OR fields LIKE "% pay%" OR fields LIKE "% salary%"
                       OR fields LIKE "% salaries%" OR fields LIKE "% discriminat%" OR fields LIKE "% exploit%" OR
                    fields LIKE "% empower%" OR fields LIKE "% disadvantag%" OR fields LIKE "% dignity%"
                       OR fields LIKE "% violence%" OR fields LIKE "% labor force%" OR fields LIKE "% labour force%" OR
                    fields LIKE "% work force%" OR fields LIKE "% leadership%" OR fields LIKE "% managerial%"
                       OR fields LIKE "% underrepresent%" OR fields LIKE "% under represent%" OR
                    fields LIKE "% representation%" OR fields LIKE "% diversity%" OR fields LIKE "% domestic violence%")
               THEN TRUE
           WHEN
               (title_abs LIKE "% sexual and reproductive health%" OR title_abs LIKE "% sexual exploitation%" OR
                title_abs LIKE "% sexual violence%" OR title_abs LIKE "% child marriage%"
                   OR title_abs LIKE "% gender quota%" OR title_abs LIKE "% female genital mutilation%" OR
                title_abs LIKE "% intimate partner violence%" OR title_abs LIKE "% prostitut%"
                   OR fields LIKE "% sexual and reproductive health%" OR fields LIKE "% sexual exploitation%" OR
                fields LIKE "% sexual violence%" OR fields LIKE "% child marriage%"
                   OR fields LIKE "% gender quota%" OR fields LIKE "% female genital mutilation%" OR
                fields LIKE "% intimate partner violence%" OR fields LIKE "% prostitut%")
               THEN TRUE
           ELSE FALSE END AS sdg_5_gender_equality,
       CASE
           WHEN
               (title_abs LIKE "% clean water%" OR title_abs LIKE "% drinking water%" OR
                title_abs LIKE "% fresh water%" OR
                title_abs LIKE "% wastewater%" OR title_abs LIKE "% waste water%"
                   OR title_abs LIKE "% water quality%" OR title_abs LIKE "% sanitation%" OR
                title_abs LIKE "% water supply%" OR title_abs LIKE "% water treatment%" OR
                title_abs LIKE "% wastewater treatment%"
                   OR title_abs LIKE "% water security%" OR title_abs LIKE "% water resource management%" OR
                title_abs LIKE "% water resources management%" OR title_abs LIKE "% sewer%" OR
                title_abs LIKE "% greywater%"
                   OR fields LIKE "% clean water%" OR fields LIKE "% drinking water%" OR fields LIKE "% fresh water%" OR
                fields LIKE "% wastewater%" OR fields LIKE "% waste water%"
                   OR fields LIKE "% water quality%" OR fields LIKE "% sanitation%" OR fields LIKE "% water supply%" OR
                fields LIKE "% water treatment%" OR fields LIKE "% wastewater treatment%"
                   OR fields LIKE "% water security%" OR fields LIKE "% water resource management%" OR
                fields LIKE "% water resources management%" OR fields LIKE "% sewer%" OR fields LIKE "% greywater%")
               THEN TRUE
           ELSE FALSE END as sdg_6_clean_water,
       CASE
           WHEN
                   (title_abs NOT LIKE "% small cell network%" AND
                    title_abs NOT LIKE "% distributed antenna systems%") AND
                   (title_abs LIKE "% energy%" OR title_abs LIKE "% fuel%" OR title_abs LIKE "% electricity%") AND
                   (title_abs LIKE "% renewable%" OR title_abs LIKE "% sustainable%" OR title_abs LIKE "% clean%" OR
                    title_abs LIKE "% affordable%" OR title_abs LIKE "% low carbon%"
                       OR title_abs LIKE "% reliable%" OR title_abs LIKE "% green house gas emissions%" OR
                    title_abs LIKE "% greenhouse gas emissions%")
               THEN TRUE
           WHEN
                   (fields NOT LIKE "% small cell network%" AND fields NOT LIKE "% distributed antenna systems%") AND
                   (fields LIKE "% energy%" OR fields LIKE "% fuel%" OR fields LIKE "% electricity%") AND
                   (fields LIKE "% renewable%" OR fields LIKE "% sustainable%" OR fields LIKE "% clean%" OR
                    fields LIKE "% affordable%" OR fields LIKE "% low carbon%"
                       OR fields LIKE "% reliable%" OR fields LIKE "% green house gas emissions%" OR
                    fields LIKE "% greenhouse gas emissions%")
               THEN TRUE
           WHEN
               (title_abs LIKE "% wind power%" OR title_abs LIKE "% solar power%" OR title_abs LIKE "% hydro power%" OR
                title_abs LIKE "% renewable power%" OR title_abs LIKE "% hydroelectric%"
                   OR title_abs LIKE "% green energy%" OR title_abs LIKE "% sustainable energy%" OR
                title_abs LIKE "% geothermal%"
                   OR title_abs LIKE "% energy intensity%" OR title_abs LIKE "% clean%fossil fuel technology%" OR
                title_abs LIKE "% wave power%"
                   OR title_abs LIKE "% wind turbine%" OR title_abs LIKE "% energy innovation%" OR
                title_abs LIKE "% energiewnde%"
                   OR fields LIKE "% wind power%" OR fields LIKE "% solar power%" OR fields LIKE "% hydro power%" OR
                fields LIKE "% renewable power%" OR fields LIKE "% hydroelectric%"
                   OR fields LIKE "% green energy%" OR fields LIKE "% sustainable energy%" OR
                fields LIKE "% geothermal%"
                   OR fields LIKE "% energy intensity%" OR fields LIKE "% clean%fossil fuel technology%" OR
                fields LIKE "% wave power%"
                   OR fields LIKE "% wind turbine%" OR fields LIKE "% energy innovation%" OR
                fields LIKE "% energiewnde%")
               THEN TRUE
           ELSE FALSE END as sdg_7_clean_energy,
       CASE
           WHEN
                   (title_abs LIKE "% employment%" OR title_abs LIKE "% jobs%" OR title_abs LIKE "% labour%" OR
                    title_abs LIKE "% labor%" OR title_abs LIKE "% worker%"
                       OR title_abs LIKE "% wage%" OR title_abs LIKE "% unemployment%" OR
                    title_abs LIKE "% prosperity%") AND
                   (title_abs LIKE "% sustain%" OR title_abs LIKE "% development%" OR
                    title_abs LIKE "% productivity%" OR
                    title_abs LIKE "% inclusive%" OR title_abs LIKE "% decent")
               THEN TRUE
           WHEN
                   (fields LIKE "% employment%" OR fields LIKE "% jobs%" OR fields LIKE "% labour%" OR
                    fields LIKE "% labor%" OR fields LIKE "% worker%"
                       OR fields LIKE "% wage%" OR fields LIKE "% unemployment%" OR fields LIKE "% prosperity%") AND
                   (fields LIKE "% sustain%" OR fields LIKE "% development%" OR fields LIKE "% productivity%" OR
                    fields LIKE "% inclusive%" OR fields LIKE "% decent")
               THEN TRUE
           WHEN
               (title_abs LIKE "% economic growth%" OR title_abs LIKE "% economic development%" OR
                title_abs LIKE "% labor productivity%" OR title_abs LIKE "% labour productivity%"
                   OR title_abs LIKE "% gdp growth%" OR title_abs LIKE "% gdp per capita%" OR
                title_abs LIKE "% economic productivity%" OR title_abs LIKE "% aid for trade%"
                   OR title_abs LIKE "% decent job%" OR title_abs LIKE "% decent work%" OR
                title_abs LIKE "% quality job%"
                   OR title_abs LIKE "% living wage%" OR title_abs LIKE "% minimum wage%" OR
                title_abs LIKE "% precarious employment%"
                   OR fields LIKE "% economic growth%" OR fields LIKE "% economic development%" OR
                fields LIKE "% labor productivity%" OR fields LIKE "% labour productivity%"
                   OR fields LIKE "% gdp growth%" OR fields LIKE "% gdp per capita%" OR
                fields LIKE "% economic productivity%" OR fields LIKE "% aid for trade%"
                   OR fields LIKE "% decent job%" OR fields LIKE "% decent work%" OR fields LIKE "% quality job%"
                   OR fields LIKE "% living wage%" OR fields LIKE "% minimum wage%" OR
                fields LIKE "% precarious employment%")
               THEN TRUE
           ELSE FALSE END as sdg_8_decent_work,
       CASE
           WHEN
                   (title_abs LIKE "% infrastructure%" OR title_abs LIKE "% industrialization%" OR
                    title_abs LIKE "% industrialisation%" OR title_abs LIKE "% manufacturing%"
                       OR title_abs LIKE "% internet access%" OR title_abs LIKE "% broadband%" OR
                    title_abs LIKE "% mobile connectivity%") AND
                   (title_abs LIKE "% sustainable%" OR title_abs LIKE "% inclusive%" OR
                    title_abs LIKE "% economic development%" OR title_abs LIKE "% economic growth%"
                       OR title_abs LIKE "% developing countr%" OR title_abs LIKE "% developing nation%")
               THEN TRUE
           WHEN
                   (fields LIKE "% infrastructure%" OR fields LIKE "% industrialization%" OR
                    fields LIKE "% industrialisation%" OR fields LIKE "% manufacturing%"
                       OR fields LIKE "% internet access%" OR fields LIKE "% broadband%" OR
                    fields LIKE "% mobile connectivity%") AND
                   (fields LIKE "% sustainable%" OR fields LIKE "% inclusive%" OR
                    fields LIKE "% economic development%" OR
                    fields LIKE "% economic growth%"
                       OR fields LIKE "% developing countr%" OR fields LIKE "% developing nation%")
               THEN TRUE
           WHEN
                   (title_abs LIKE "% scientific research%" OR title_abs LIKE "% research and development%" OR
                    title_abs LIKE "% r&d%" OR title_abs LIKE "% innovation%"
                       OR title_abs LIKE "% entrepreneur%" OR title_abs LIKE "% creative industr%" OR
                    title_abs LIKE "% creative economy%" OR title_abs LIKE "% service industr%"
                       OR title_abs LIKE "% service economy%" OR title_abs LIKE "% tourism%") AND
                   (title_abs LIKE "% economic development%" OR title_abs LIKE "% economic growth%"
                       OR title_abs LIKE "% developing countr%" OR title_abs LIKE "% developing nation%")
               THEN TRUE
           WHEN
                   (fields LIKE "% scientific research%" OR fields LIKE "% research and development%" OR
                    fields LIKE "% r&d%" OR fields LIKE "% innovation%"
                       OR fields LIKE "% entrepreneur%" OR fields LIKE "% creative industr%" OR
                    fields LIKE "% creative economy%" OR fields LIKE "% service industr%"
                       OR fields LIKE "% service economy%" OR fields LIKE "% tourism%") AND
                   (fields LIKE "% economic development%" OR fields LIKE "% economic growth%"
                       OR fields LIKE "% developing countr%" OR fields LIKE "% developing nation%")
               THEN TRUE
           WHEN
               (title_abs LIKE "% industrial diversification%" OR title_abs LIKE "% sustainable transport%" OR
                title_abs LIKE "% sustainable transit%"
                   OR title_abs LIKE "% public transport infrastructure%" OR
                title_abs LIKE "% public transportation infrastructure%"
                   OR title_abs LIKE "% infrastructure development%" OR
                title_abs LIKE "% infrastructural development%" OR
                title_abs LIKE "% infrastructure construction%"
                   OR title_abs LIKE "% belt and road%" OR title_abs LIKE "% transborder infrastructure%"
                   OR fields LIKE "% industrial diversification%" OR fields LIKE "% sustainable transport%" OR
                fields LIKE "% sustainable transit%"
                   OR fields LIKE "% public transport infrastructure%" OR
                fields LIKE "% public transportation infrastructure%"
                   OR fields LIKE "% infrastructure development%" OR fields LIKE "% infrastructural development%" OR
                fields LIKE "% infrastructure construction%"
                   OR fields LIKE "% belt and road%" OR fields LIKE "% transborder infrastructure%")
               THEN TRUE
           ELSE FALSE END as sdg_9_infrastructure_innovation,
       CASE
           WHEN
                   (title_abs NOT LIKE "% health%") AND
                   (title_abs LIKE "% international trade%" OR title_abs LIKE "% world trade%" OR
                    title_abs LIKE "% free trade%" OR title_abs LIKE "% trade barrier%"
                       OR title_abs LIKE "% inequalit%" OR title_abs LIKE "% equalit%" OR
                    title_abs LIKE "% equal opportunity%") AND
                   (title_abs LIKE "% developing countr%" OR title_abs LIKE "% developing nation%" OR
                    title_abs LIKE "% financ%" OR title_abs LIKE "% econom%"
                       OR title_abs LIKE "% socioeconom%" OR title_abs LIKE "% income%")
               THEN TRUE
           WHEN
                   (fields NOT LIKE "% health%") AND
                   (fields LIKE "% international trade%" OR fields LIKE "% world trade%" OR
                    fields LIKE "% free trade%" OR
                    fields LIKE "% trade barrier%"
                       OR fields LIKE "% inequalit%" OR fields LIKE "% equalit%" OR
                    fields LIKE "% equal opportunity%") AND
                   (fields LIKE "% developing countr%" OR fields LIKE "% developing nation%" OR
                    fields LIKE "% financ%" OR
                    fields LIKE "% econom%"
                       OR fields LIKE "% socioeconom%" OR fields LIKE "% income%")
               THEN TRUE
           WHEN
                   (title_abs NOT LIKE "% health%") AND
                   (title_abs LIKE "% tariff%" OR title_abs LIKE "% tax %" OR title_abs LIKE "% taxation%" OR
                    title_abs LIKE "% duty free%") AND
                   (title_abs LIKE "% developing countr%" OR title_abs LIKE "% developing state%" OR
                    title_abs LIKE "% developing nation%" OR title_abs LIKE "% economic development%")
               THEN TRUE
           WHEN
                   (fields NOT LIKE "% health%") AND
                   (fields LIKE "% tariff%" OR fields LIKE "% tax %" OR fields LIKE "% taxation%" OR
                    fields LIKE "% duty free%") AND
                   (fields LIKE "% developing countr%" OR fields LIKE "% developing state%" OR
                    fields LIKE "% developing nation%" OR fields LIKE "% economic development%")
               THEN TRUE
           WHEN
                   (title_abs NOT LIKE "% health%" and fields NOT LIKE "% health%") AND
                   (title_abs LIKE "% income disparity%" OR title_abs LIKE "% affordable housing%" OR
                    title_abs LIKE "% gini index%" OR title_abs LIKE "% gini coefficient%"
                       OR fields LIKE "% income disparity%" OR fields LIKE "% affordable housing%" OR
                    fields LIKE "% gini index%" OR fields LIKE "% gini coefficient%")
               THEN TRUE
           ELSE FALSE END as sdg_10_reduced_inequalities,
       CASE
           WHEN
                   (title_abs LIKE "% urbanisation%" OR title_abs LIKE "% urbanization%" OR title_abs LIKE "% city%" OR
                    title_abs LIKE "% cities%"
                       OR title_abs LIKE "% urban%" OR title_abs LIKE "% slum%") AND
                   (title_abs LIKE "% resilen%" OR title_abs LIKE "% sustain%" OR title_abs LIKE "% inclusiv%" OR
                    title_abs LIKE "% particulate matter%"
                       OR title_abs LIKE "% public transport%" OR title_abs LIKE "% public space%" OR
                    title_abs LIKE "% green space%" OR title_abs LIKE "% waste management%"
                       OR title_abs LIKE "% waste collection%" OR title_abs LIKE "% access%" OR
                    title_abs LIKE "% pollution%" OR title_abs LIKE "% climate change%")
               THEN TRUE
           WHEN
                   (fields LIKE "% urbanisation%" OR fields LIKE "% urbanization%" OR fields LIKE "% city%" OR
                    fields LIKE "% cities%"
                       OR fields LIKE "% urban%" OR fields LIKE "% slum%") AND
                   (fields LIKE "% resilen%" OR fields LIKE "% sustain%" OR fields LIKE "% inclusiv%" OR
                    fields LIKE "% particulate matter%"
                       OR fields LIKE "% public transport%" OR fields LIKE "% public space%" OR
                    fields LIKE "% green space%" OR fields LIKE "% waste management%"
                       OR fields LIKE "% waste collection%" OR fields LIKE "% access%" OR fields LIKE "% pollution%" OR
                    fields LIKE "% climate change%")
               THEN TRUE
           WHEN
               (title_abs LIKE "% affordable housing%" OR title_abs LIKE "% sustainable communit%" OR
                title_abs LIKE "% sustainable building%" OR title_abs LIKE "% gentrification%"
                   OR fields LIKE "% affordable housing%" OR fields LIKE "% sustainable communit%" OR
                fields LIKE "% sustainable building%" OR fields LIKE "% gentrification%")
               THEN TRUE
           ELSE FALSE END as sdg_11_sustainable_cities,
       CASE
           WHEN
                   (title_abs NOT LIKE "% health%" and title_abs NOT LIKE "% metabol%") AND
                   (title_abs LIKE "% production%" OR title_abs LIKE "% waste%" OR
                    title_abs LIKE "% natural resources%") AND
                   (title_abs LIKE "% recycl%" OR title_abs LIKE "% sustainable%" OR title_abs LIKE "% efficienc%")
               THEN TRUE
           WHEN
                   (fields NOT LIKE "% health%" and fields NOT LIKE "% metabol%") AND
                   (fields LIKE "% production%" OR fields LIKE "% waste%" OR fields LIKE "% natural resources%") AND
                   (fields LIKE "% recycl%" OR fields LIKE "% sustainable%" OR fields LIKE "% efficienc%")
               THEN TRUE
           WHEN
               (title_abs LIKE "% sustainable design%" OR title_abs LIKE "% industrial ecology%"
                   OR title_abs LIKE "% industrial waste%" OR title_abs LIKE "% toxic waste%"
                   OR title_abs LIKE "% material consumption%" OR title_abs LIKE "% green consumption%"
                   OR title_abs LIKE "% sustainable tourism%" OR title_abs LIKE "% fossil fuel subsid%"
                   OR title_abs LIKE "% sustainability report%" OR fields LIKE "% sustainable design%"
                   OR fields LIKE "% industrial ecology%" OR fields LIKE "% industrial waste%"
                   OR fields LIKE "% toxic waste%" OR fields LIKE "% material consumption%"
                   OR fields LIKE "% green consumption%" OR fields LIKE "% sustainable tourism%"
                   OR fields LIKE "% fossil fuel subsid%" OR fields LIKE "% sustainability report%")
               THEN TRUE
           ELSE FALSE END as sdg_12_responsible_consumption,
       CASE
           WHEN
                   (title_abs LIKE "% climate%") AND
                   (title_abs LIKE "% change%" OR title_abs LIKE "% extreme%" OR title_abs LIKE "% weather%"
                       OR title_abs LIKE "% warming%")
               THEN TRUE
           WHEN
                   (fields LIKE "% climate%") AND
                   (fields LIKE "% change%" OR fields LIKE "% extreme%" OR fields LIKE "% weather%"
                       OR fields LIKE "% warming%")
               THEN TRUE
           WHEN
               (title_abs LIKE "% global warming%" OR title_abs LIKE "% greenhouse gas%" OR
                title_abs LIKE "% climate action%" OR title_abs LIKE "% climate mitigation%"
                   OR title_abs LIKE "% climate related hazards%" OR title_abs LIKE "% extreme weather%"
                   OR title_abs LIKE "% ocean warming%" OR title_abs LIKE "% sea level rise%"
                   OR title_abs LIKE "% rising sea level%" OR title_abs LIKE "% green climate fund%"
                   OR title_abs LIKE "% climate finance%" OR title_abs LIKE "% climate adaptation%"
                   OR fields LIKE "% global warming%" OR fields LIKE "% greenhouse gas%"
                   OR fields LIKE "% climate action%" OR fields LIKE "% climate mitigation%"
                   OR fields LIKE "% climate related hazards%" OR fields LIKE "% extreme weather%"
                   OR fields LIKE "% ocean warming%" OR fields LIKE "% sea level rise%"
                   OR fields LIKE "% rising sea level%" OR fields LIKE "% green climate fund%"
                   OR fields LIKE "% climate finance%" OR fields LIKE "% climate adaptation%")
               THEN TRUE
           ELSE FALSE END as sdg_13_climate_action,
       CASE
           WHEN
                   (title_abs LIKE "% ocean%" OR title_abs LIKE "% marine%" OR title_abs LIKE "% sea %"
                       OR title_abs LIKE "% seas %" OR title_abs LIKE "% coast%" OR title_abs LIKE "% mangrove%") AND
                   (title_abs LIKE "% sustain%" OR title_abs LIKE "% conserv%" OR title_abs LIKE "% ecosystem%"
                       OR title_abs LIKE "% pollution%" OR title_abs LIKE "% biodiversity%"
                       OR title_abs LIKE "% eutrophication%" OR title_abs LIKE "% deoxygenation%"
                       OR title_abs LIKE "% warming%" OR title_abs LIKE "% hypoxia%"
                       OR title_abs LIKE "% dead zone%" OR title_abs LIKE "% acidification%")
               THEN TRUE
           WHEN
                   (fields LIKE "% ocean%" OR fields LIKE "% marine%" OR fields LIKE "% sea %" OR fields LIKE "% seas %"
                       OR fields LIKE "% coast%" OR fields LIKE "% mangrove%") AND
                   (fields LIKE "% sustain%" OR fields LIKE "% conserv%" OR fields LIKE "% ecosystem%"
                       OR fields LIKE "% pollution%" OR fields LIKE "% biodiversity%" OR fields LIKE "% eutrophication%"
                       OR fields LIKE "% deoxygenation%" OR fields LIKE "% warming%" OR fields LIKE "% hypoxia%"
                       OR fields LIKE "% dead zone%" OR fields LIKE "% acidification%")
               THEN TRUE

           WHEN
               (title_abs LIKE "% coral bleaching%" OR title_abs LIKE "% unregulated fishin%" OR
                title_abs LIKE "% sustainable fishing%"
                   OR title_abs LIKE "% over fishing%" OR title_abs LIKE "% fisheries management%"
                   OR fields LIKE "% coral bleaching%" OR fields LIKE "% unregulated fishin%"
                   OR fields LIKE "% sustainable fishing%" OR fields LIKE "% over fishing%"
                   OR fields LIKE "% fisheries management%")
               THEN TRUE
           ELSE FALSE END as sdg_14_life_below_water,
       CASE
           WHEN
                   (title_abs LIKE "% forest%" OR title_abs LIKE "% wetland%" OR title_abs LIKE "% mountain%"
                       OR title_abs LIKE "% dryland%" OR title_abs LIKE "% fresh water%" OR
                    title_abs LIKE "% freshwater%"
                       OR title_abs LIKE "% wildlife%" OR title_abs LIKE "% desert%") AND
                   (title_abs LIKE "% biodiversity%" OR title_abs LIKE "% ecosystem%" OR title_abs LIKE "% ecology%")
               THEN TRUE
           WHEN
                   (fields LIKE "% forest%" OR fields LIKE "% wetland%" OR fields LIKE "% mountain%"
                       OR fields LIKE "% dryland%" OR fields LIKE "% fresh water%" OR fields LIKE "% freshwater%"
                       OR fields LIKE "% wildlife%" OR fields LIKE "% desert%") AND
                   (fields LIKE "% biodiversity%" OR fields LIKE "% ecosystem%" OR fields LIKE "% ecology%")
               THEN TRUE
           WHEN
               (title_abs LIKE "% terrestrial ecosystems%" OR title_abs LIKE "% desertification%"
                   OR title_abs LIKE "% invasive species%" OR title_abs LIKE "% red list index%"
                   OR title_abs LIKE "% nagoya protocol on access to genetic resources%"
                   OR title_abs LIKE "% deforestation%" OR title_abs LIKE "% reforestation%"
                   OR title_abs LIKE "% afforestation%" OR title_abs LIKE "% sustainable forest management%"
                   OR title_abs LIKE "% degraded land%" OR title_abs LIKE "% degraded soil%"
                   OR title_abs LIKE "% mountain green cover index%" OR title_abs LIKE "% poaching%"
                   OR title_abs LIKE "% trafficking of protected species%"
                   OR title_abs LIKE "% illegal wildlife products%"
                   OR title_abs LIKE "% threatened species%" OR title_abs LIKE "% extinct species%"
                   OR fields LIKE "% terrestrial ecosystems%" OR fields LIKE "% desertification%"
                   OR fields LIKE "% invasive species%" OR fields LIKE "% red list index%"
                   OR fields LIKE "% nagoya protocol on access to genetic resources%"
                   OR fields LIKE "% deforestation%" OR fields LIKE "% reforestation%"
                   OR fields LIKE "% afforestation%" OR fields LIKE "% sustainable forest management%"
                   OR fields LIKE "% degraded land%" OR fields LIKE "% degraded soil%"
                   OR fields LIKE "% mountain green cover index%" OR fields LIKE "% poaching%"
                   OR fields LIKE "% trafficking of protected species%"
                   OR fields LIKE "% illegal wildlife products%"
                   OR fields LIKE "% threatened species%" OR title_abs LIKE "% extinct species%")
               THEN TRUE
           ELSE FALSE END as sdg_15_life_on_land,
       CASE
           WHEN
                   (title_abs LIKE "% justic%" OR title_abs LIKE "% governance%" OR title_abs LIKE "% democrac%"
                       OR title_abs LIKE "% crime%" OR title_abs LIKE "% criminal%" OR title_abs LIKE "% judicia%"
                       OR title_abs LIKE "% corrupt%") AND
                   (title_abs LIKE "% institution%" OR title_abs LIKE "% accountable%" OR
                    title_abs LIKE "% transparent%"
                       OR title_abs LIKE "% inclusive%" OR title_abs LIKE "% open%")
               THEN TRUE
           WHEN
                   (fields LIKE "% justic%" OR fields LIKE "% governance%" OR fields LIKE "% democrac%"
                       OR fields LIKE "% crime%" OR fields LIKE "% criminal%" OR fields LIKE "% judicia%"
                       OR fields LIKE "% corrupt%") AND
                   (fields LIKE "% institution%" OR fields LIKE "% accountable%" OR fields LIKE "% transparent%"
                       OR fields LIKE "% inclusive%" OR fields LIKE "% open%")
               THEN TRUE
           WHEN
               (title_abs LIKE "% arms traffic%" OR title_abs LIKE "% illicit arms trade%"
                   OR title_abs LIKE "% hate crime%" OR title_abs LIKE "% human trafficking%"
                   OR title_abs LIKE "% illegal arms%" OR title_abs LIKE "% justice for all%"
                   OR title_abs LIKE "% organized crime%" OR title_abs LIKE "% organised crime%"
                   OR title_abs LIKE "% false confession%" OR title_abs LIKE "% torture%"
                   OR title_abs LIKE "% armed conflict%" OR title_abs LIKE "% civil conflict%"
                   OR title_abs LIKE "% peacekeeping%" OR title_abs LIKE "% geneva convention%"
                   OR title_abs LIKE "% genocid%" OR title_abs LIKE "% homicid%"
                   OR title_abs LIKE "% effictive rule of law%" OR title_abs LIKE "% fundamental freedom%"
                   OR title_abs LIKE "% peacefull society%" OR title_abs LIKE "% independent judiciary%"
                   OR title_abs LIKE "% child trafficking%" OR title_abs LIKE "% voting right%"
                   OR title_abs LIKE "% right to vote%"
                   OR fields LIKE "% arms traffic%" OR fields LIKE "% illicit arms trade%"
                   OR fields LIKE "% hate crime%" OR fields LIKE "% human trafficking%"
                   OR fields LIKE "% illegal arms%" OR fields LIKE "% justice for all%"
                   OR fields LIKE "% organized crime%" OR fields LIKE "% organised crime%"
                   OR fields LIKE "% false confession%" OR fields LIKE "% torture%"
                   OR fields LIKE "% armed conflict%" OR fields LIKE "% civil conflict%"
                   OR fields LIKE "% peacekeeping%" OR fields LIKE "% geneva convention%"
                   OR fields LIKE "% genocid%" OR fields LIKE "% homicid%"
                   OR fields LIKE "% effictive rule of law%" OR fields LIKE "% fundamental freedom%"
                   OR fields LIKE "% peacefull society%" OR fields LIKE "% independent judiciary%"
                   OR fields LIKE "% child trafficking%" OR fields LIKE "% voting right%"
                   OR fields LIKE "% right to vote%")
               THEN TRUE
           ELSE FALSE END as sdg_16_peace_institutions,
       CASE
           WHEN
               (title_abs LIKE "% world bank%" OR title_abs LIKE "% oda %"
                   OR title_abs LIKE "% official development assistance%"
                   OR title_abs LIKE "% private investment%" OR title_abs LIKE "% economic imabalanc%"
                   OR title_abs LIKE "% foreign direct investment%" OR title_abs LIKE "% fdi %"
                   OR title_abs LIKE "% world trade organization%" OR title_abs LIKE "% world trade organisation%"
                   OR title_abs LIKE "% wto %" OR title_abs LIKE "% public private partnership%"
                   OR title_abs LIKE "% bilateral investment%" OR title_abs LIKE "% access to the internet%"
                   OR title_abs LIKE "% international cooperation%" OR title_abs LIKE "% international aid%"
                   OR title_abs LIKE "% civil society partnership%"
                   OR fields LIKE "% world bank%" OR fields LIKE "% oda %"
                   OR fields LIKE "% official development assistance%"
                   OR fields LIKE "% private investment%" OR fields LIKE "% economic imabalanc%"
                   OR fields LIKE "% foreign direct investment%" OR fields LIKE "% fdi %"
                   OR fields LIKE "% world trade organization%" OR fields LIKE "% world trade organisation%"
                   OR fields LIKE "% wto %" OR fields LIKE "% public private partnership%"
                   OR fields LIKE "% bilateral investment%" OR fields LIKE "% access to the internet%"
                   OR fields LIKE "% international cooperation%" OR fields LIKE "% international aid%"
                   OR fields LIKE "% civil society partnership%")
               THEN TRUE
           WHEN
               (title_abs LIKE "% sdg17%" OR title_abs LIKE "% enhanced international cooperation%"
                   OR title_abs LIKE "% doha development agenda%"
                   OR title_abs LIKE "% global partnership for sustainable development%"
                   OR fields LIKE "% sdg17%" OR fields LIKE " enhanced international cooperation%"
                   OR fields LIKE "% doha development agenda%"
                   OR fields LIKE "% global partnership for sustainable development%")
               THEN TRUE
           ELSE FALSE END as sdg_17_partnerships,
       CASE
           WHEN
               title_abs LIKE "% sustainab%" OR fields LIKE "% sustainab%"
               THEN TRUE
           ELSE FALSE END as sustainab_title_abs_field,
       CASE
           WHEN
               title_abs LIKE "% development %" OR fields LIKE "% development %"
               THEN TRUE
           ELSE FALSE END as development_title_abs_field,
       CASE
           WHEN
                       title_abs LIKE "% sustainable development goal%" or fields LIKE "% sustainable development goal%"
                   OR title_abs LIKE "% sdg%" or fields LIKE "% sdg%"
               THEN TRUE
           ELSE FALSE END as sdgs_ref_in_title_abs,
        title_abs,
        fields
FROM titleabs