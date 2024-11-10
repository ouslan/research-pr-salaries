from sqlmodel import Field, SQLModel
from typing import Optional

class DP03Table(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    year: int
    zipcode: str
    population_16_years_and_over: int
    in_labor_force: int
    civilian_labor_force: int
    employed: int
    unemployed: int
    armed_forces: int
    not_in_labor_force: int
    civilian_labor_force2: int
    females_16_years_and_over: int
    civilian_labor_force_f: int
    civilian_labor_forcef2: int
    employed_f: int
    own_children_under_6_years: int
    all_parents_in_family_in_labor_f: int
    own_children_6_to_17_years: int
    var18: int
    workers_16_years_and_over: int
    car_truck_or_van_nn_drove_alone: int
    car_truck_or_van_nn_carpooled: int
    public_transportation_excluding_: int
    walked: int
    other_means: int
    worked_at_home: int
    mean_travel_time_to_work_minutes: Optional[float] = None
    civilian_employed_population_16_: int
    management_business_science_and_: int
    service_occupations: int
    sales_and_office_occupations: int
    natural_resources_construction_a: int
    production_transportation_and_ma: int
    var33: int
    agriculture_forestry_fishing_and: int
    construction: int
    manufacturing: int
    wholesale_trade: int
    retail_trade: int
    transportation_and_warehousing_a: int
    information: int
    finance_and_insurance_and_real_e: int
    professional_scientific_and_mana: int
    educational_services_and_health_: int
    arts_entertainment_and_recreatio: int
    other_services_except_public_adm: int
    public_administration: int
    p_agriculture_forestry_fishing_a: Optional[float] = None
    p_construction: Optional[float] = None
    p_manufacturing: Optional[float] = None
    p_wholesale_trade: Optional[float] = None
    p_retail_trade: Optional[float] = None
    p_transportation_and_warehousing: Optional[float] = None
    p_information: Optional[float] = None
    p_finance_and_insurance_and_real: Optional[float] = None
    p_professional_scientific_and_ma: Optional[float] = None
    p_educational_services_and_healt: Optional[float] = None
    p_arts_entertainment_and_recreat: Optional[float] = None
    p_other_services_except_public_a: Optional[float] = None
    p_public_administration: Optional[float] = None
    var60: int
    private_wage_and_salary_workers: int
    government_workers: int
    selfnemployed_in_own_not_incorpo: int
    unpaid_family_workers: int
    total_households: int
    less_than_10000: int
    n_10000_to_14999: int
    n_15000_to_24999: int
    n_25000_to_34999: int
    n_35000_to_49999: int
    n_50000_to_74999: int
    n_75000_to_99999: int
    n_100000_to_149999: int
    n_150000_to_199999: int
    n_200000_or_more: int
    p_less_than_10000: Optional[float] = None
    p_10000_to_14999: Optional[float] = None
    p_15000_to_24999: Optional[float] = None
    p_25000_to_34999: Optional[float] = None
    p_35000_to_49999: Optional[float] = None
    p_50000_to_74999: Optional[float] = None
    p_75000_to_99999: Optional[float] = None
    p_100000_to_149999: Optional[float] = None 
    p_150000_to_199999: Optional[float] = None
    p_200000_or_more: Optional[float] = None
    median_household_income_dollars: Optional[float] = None
    mean_household_income_dollars: Optional[float] = None
    with_earnings: int
    mean_earnings_dollars: Optional[float] = None 
    with_social_security: int
    mean_social_security_income_doll: Optional[float] = None
    with_retirement_income: int
    mean_retirement_income_dollars: Optional[float] = None
    with_supplemental_security_incom: int
    mean_supplemental_security_incom: Optional[float] = None
    with_cash_public_assistance_inco: int
    p_with_earnings: Optional[float] = None
    p_with_social_security: Optional[float] = None
    p_retirement: Optional[float] = None
    p_snap: Optional[float] = None
    mean_cash_public_assistance_inco: Optional[float] = None
    with_food_stampsnap_benefits_in_: int
    families: int
    var104: int
    n_10000_to_n_14999: int
    n_15000_to_n_24999: int
    n_25000_to_n_34999: int
    n_35000_to_n_49999: int
    n_50000_to_n_74999: int
    n_75000_to_n_99999: int 
    n_100000_to_n_149999: int
    n_150000_to_n_199999: int
    var113: int
    median_family_income_dollars: Optional[float] = None
    mean_family_income_dollars: Optional[float] = None
    per_capita_income_dollars: Optional[float] = None
    nonfamily_households: int
    median_nonfamily_income_dollars: Optional[float] = None
    mean_nonfamily_income_dollars: Optional[float] = None
    median_earnings_for_workers_doll: Optional[float] = None
    median_earnings_for_male_fullnti: Optional[float] = None
    median_earnings_for_female_fulln: Optional[float] = None
    qtr: int
    food_security: Optional[float] = None

def create_dp03(engine):
    SQLModel.metadata.create_all(engine)