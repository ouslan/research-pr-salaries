import duckdb

con = duckdb.connect("data.ddb")

con.sql(
    """
    DROP SEQUENCE IF EXISTS dp03table_id_seq;
    DROP SEQUENCE IF EXISTS munitable_id_seq;
    DROP SEQUENCE IF EXISTS zipstable_id_seq;
    CREATE SEQUENCE munitable_id_seq START WITH 1;
    CREATE SEQUENCE dp03table_id_seq START WITH 1;
    CREATE SEQUENCE zipstable_id_seq START WITH 1;
    """
)

con.sql(
    """
CREATE TABLE IF NOT EXISTS "dp03table" (
    id INTEGER PRIMARY KEY DEFAULT nextval('dp03table_id_seq'),
    year INT NOT NULL,
    zipcode VARCHAR(255) NOT NULL,
    population_16_years_and_over INT,
    in_labor_force INT,
    civilian_labor_force INT,
    employed INT,
    unemployed INT,
    armed_forces INT,
    not_in_labor_force INT,
    civilian_labor_force2 INT,
    females_16_years_and_over INT,
    civilian_labor_force_f INT,
    civilian_labor_forcef2 INT,
    employed_f INT,
    own_children_under_6_years INT,
    all_parents_in_family_in_labor_f INT,
    own_children_6_to_17_years INT,
    var18 INT,
    workers_16_years_and_over INT,
    car_truck_or_van_nn_drove_alone INT,
    car_truck_or_van_nn_carpooled INT,
    public_transportation_excluding_ INT,
    walked INT,
    other_means INT,
    worked_at_home INT,
    mean_travel_time_to_work_minutes FLOAT,
    civilian_employed_population_16_ INT,
    management_business_science_and_ INT,
    service_occupations INT,
    sales_and_office_occupations INT,
    natural_resources_construction_a INT,
    production_transportation_and_ma INT,
    var33 INT,
    agriculture_forestry_fishing_and INT,
    construction INT,
    manufacturing INT,
    wholesale_trade INT,
    retail_trade INT,
    transportation_and_warehousing_a INT,
    information INT,
    finance_and_insurance_and_real_e INT,
    professional_scientific_and_mana INT,
    educational_services_and_health_ INT,
    arts_entertainment_and_recreatio INT,
    other_services_except_public_adm INT,
    public_administration INT,
    p_agriculture_forestry_fishing_a FLOAT,
    p_construction FLOAT,
    p_manufacturing FLOAT,
    p_wholesale_trade FLOAT,
    p_retail_trade FLOAT,
    p_transportation_and_warehousing FLOAT,
    p_information FLOAT,
    p_finance_and_insurance_and_real FLOAT,
    p_professional_scientific_and_ma FLOAT,
    p_educational_services_and_healt FLOAT,
    p_arts_entertainment_and_recreat FLOAT,
    p_other_services_except_public_a FLOAT,
    p_public_administration FLOAT,
    var60 INT,
    private_wage_and_salary_workers INT,
    government_workers INT,
    selfnemployed_in_own_not_incorpo INT,
    unpaid_family_workers INT,
    total_households INT,
    less_than_10000 INT,
    n_10000_to_14999 INT,
    n_15000_to_24999 INT,
    n_25000_to_34999 INT,
    n_35000_to_49999 INT,
    n_50000_to_74999 INT,
    n_75000_to_99999 INT,
    n_100000_to_149999 INT,
    n_150000_to_199999 INT,
    n_200000_or_more INT,
    p_less_than_10000 FLOAT,
    p_10000_to_14999 FLOAT,
    p_15000_to_24999 FLOAT,
    p_25000_to_34999 FLOAT,
    p_35000_to_49999 FLOAT,
    p_50000_to_74999 FLOAT,
    p_75000_to_99999 FLOAT,
    p_100000_to_149999 FLOAT,
    p_150000_to_199999 FLOAT,
    p_200000_or_more FLOAT,
    median_household_income_dollars FLOAT,
    mean_household_income_dollars FLOAT,
    with_earnings INT,
    mean_earnings_dollars FLOAT,
    with_social_security INT,
    mean_social_security_income_doll FLOAT,
    with_retirement_income INT,
    mean_retirement_income_dollars FLOAT,
    with_supplemental_security_incom INT,
    mean_supplemental_security_incom FLOAT,
    with_cash_public_assistance_inco INT,
    p_with_earnings FLOAT,
    p_with_social_security FLOAT,
    p_retirement FLOAT,
    p_snap FLOAT,
    mean_cash_public_assistance_inco FLOAT,
    with_food_stampsnap_benefits_in_ INT,
    families INT,
    var104 INT,
    n_10000_to_n_14999 INT,
    n_15000_to_n_24999 INT,
    n_25000_to_n_34999 INT,
    n_35000_to_n_49999 INT,
    n_50000_to_n_74999 INT,
    n_75000_to_n_99999 INT,
    n_100000_to_n_149999 INT,
    n_150000_to_n_199999 INT,
    var113 INT,
    median_family_income_dollars FLOAT,
    mean_family_income_dollars FLOAT,
    per_capita_income_dollars FLOAT,
    nonfamily_households INT,
    median_nonfamily_income_dollars FLOAT,
    mean_nonfamily_income_dollars FLOAT,
    median_earnings_for_workers_doll FLOAT,
    median_earnings_for_male_fullnti FLOAT,
    median_earnings_for_female_fulln FLOAT,
    qtr INT,
    food_security FLOAT
);

"""
)

con.sql(
    """
    CREATE TABLE IF NOT EXISTS "munitable" (
        id INTEGER PRIMARY KEY DEFAULT nextval('munitable_id_seq'),
        municipality VARCHAR(255) NOT NULL,
        geo_id VARCHAR(255) NOT NULL
    );
"""
)

con.sql(
    """
    CREATE TABLE IF NOT EXISTS "zipstable" (
        id INTEGER PRIMARY KEY DEFAULT nextval('zipstable_id_seq'),
        zipcode VARCHAR(255) NOT NULL,
        geo_id VARCHAR(255) NOT NULL
    );
"""
)
