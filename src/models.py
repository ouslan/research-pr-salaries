import duckdb


def get_conn(db_path: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(db_path)

def init_countytable(ddb_path: str) -> None:
    conn = get_conn(ddb_path)
    conn.load_extension("spatial")
    conn.sql("DROP SEQUENCE IF EXISTS countytable_id_seq;")
    conn.sql("CREATE SEQUENCE countytable_id_seq START WITH 1;")
    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS "countytable" (
            id INTEGER PRIMARY KEY DEFAULT nextval('countytable_id_seq'),
            name VARCHAR(255) NOT NULL,
            geo_id VARCHAR(25) NOT NULL,
            geom geometry
        );
    """
    )


def init_coutsubtable(ddb_path: str) -> None:
    conn = get_conn(ddb_path)
    conn.load_extension("spatial")
    conn.sql("DROP SEQUENCE IF EXISTS countsub_id_seq;")
    conn.sql("CREATE SEQUENCE countsub_id_seq START WITH 1;")
    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS "countsubtable" (
            id INTEGER PRIMARY KEY DEFAULT nextval('countsub_id_seq'),
            name VARCHAR(255) NOT NULL,
            geo_id VARCHAR(25) NOT NULL,
            geom geometry
        );
    """
    )


def init_dp03_table(db_path: str) -> None:
    conn = get_conn(db_path=db_path)

    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS "DP03Table" (
            year INTEGER,
            state VARCHAR(3),
            zipcode VARCHAR(5),
            total_population INTEGER,
            in_labor_force INTEGER,
            unemployment INTEGER,
            own_children6 INTEGER,
            own_children17 INTEGER,
            commute_car INTEGER,
            commute_time FLOAT,
            total_house INTEGER,
            inc_less_10k INTEGER,
            inc_10k_15k INTEGER,
            inc_15k_25k INTEGER,
            inc_25k_35k INTEGER,
            inc_35k_50k INTEGER,
            inc_50k_75k INTEGER,
            inc_75k_100k INTEGER,
            inc_100k_150k INTEGER,
            inc_150k_200k INTEGER,
            inc_more_200k INTEGER,
            with_social_security INTEGER,
            food_stamp INTEGER
            );
        """
    )
