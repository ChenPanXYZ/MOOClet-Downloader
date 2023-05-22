import pandas as pd
import numpy as np
from credentials import *  

# contextual variable: time of day, 
# 0: 9am-4pm
# 1: 4pm - 9 pm
# we assign arm at 9am
# time of day: 0
# user sends reward (replies to text) at 5pm
# context is 0 because there's no context before 9AM
# time of day: 1

# Fix: when there is reward, use the reward time to match.
# If there is no reward, use the arm assign time to match.
def find_reward_variable(mooclet_name):
    cursor = conn.cursor()
    print(mooclet_name)
    cursor.execute("select id from engine_mooclet where name = %s;", [mooclet_name]);
    mooclet_id = cursor.fetchone()[0]

    cursor.execute("""
    SELECT variable_id, count(*) from engine_value where mooclet_id = %s AND policy_id IS NOT null AND version_id IS NOT null AND variable_id != 3 group by variable_id order by count(*) desc;
    """, [mooclet_id])

    variable_id = cursor.fetchone()[0]
    print(variable_id)
    cursor.execute("""
    SELECT name from engine_variable where id = %s;
    """, [variable_id])
    reward_variable_name = cursor.fetchone()[0]
    
    cursor.close()
    return reward_variable_name


mooclet_names = []


def data_downloader_local_new(mooclet_name, reward_variable_name):
    # GET DATA
    cursor = conn.cursor()
    try:
        #TODO: Think about concurrency!
        cursor.execute(
            """
            DROP VIEW IF EXISTS reward_values CASCADE;
            DROP VIEW IF EXISTS context_values CASCADE;
            DROP VIEW IF EXISTS arm_assignments CASCADE;
            DROP VIEW IF EXISTS arm_reward_merged CASCADE;
            DROP VIEW IF EXISTS arm_reward_merged_max CASCADE;
            DROP VIEW IF EXISTS contexts_merged CASCADE;
            DROP VIEW IF EXISTS contexts_merged_max CASCADE;
            """
        )
        print(f'MOOClet name: {mooclet_name}')
        print(f'Reward variable Name: {reward_variable_name}')

        reserved_variable_names = ['UR_or_TSCONTEXTUAL', 'coef_draw', 'precesion_draw', 'version', '']
        cursor.execute("SELECT array_agg(distinct(id)) from engine_variable where name = ANY(%s);", [reserved_variable_names])
        reserved_variable_ids = cursor.fetchall()[0][0]
        # get mooclet id
        cursor.execute("select id from engine_mooclet where name = %s;", [mooclet_name]);
        mooclet_id = cursor.fetchone()[0]
        print(f'MOOClet id: {mooclet_id}')
        # get reward variable id
        cursor.execute("select id from engine_variable where name = %s;", [reward_variable_name]);
        reward_variable_id = cursor.fetchone()[0]
        print(f'Reward variable id: {reward_variable_id}')

        # get the variable id used for version (arm assignments)
        cursor.execute("select id from engine_variable where name = 'version';");
        version_id = cursor.fetchone()[0]
        print(f'Version id: {version_id}')   

        # get all reward values
        cursor.execute("""
            CREATE TEMPORARY VIEW reward_values AS
            SELECT * FROM
            engine_value WHERE variable_id = %s AND mooclet_id = %s;
            """, [reward_variable_id, mooclet_id])
        
         # get all context values
        cursor.execute("""
            CREATE TEMPORARY VIEW context_values AS
            SELECT * FROM
            engine_value WHERE variable_id != %s AND mooclet_id = %s and version_id is null AND variable_id != ALL(%s);
            """, [reward_variable_id, mooclet_id, reserved_variable_ids])

        # Get all arm assignments (as a base)
        cursor.execute("""
            CREATE TEMPORARY VIEW arm_assignments AS
            SELECT * FROM
            engine_value WHERE variable_id = %s AND mooclet_id = %s;
            """, [version_id, mooclet_id])
        cursor.execute("select * from context_values")


        # First, merge arm assignments with rewards after it.
        cursor.execute("""
            CREATE TEMPORARY VIEW arm_reward_merged AS
            SELECT aa.id as assignment_id, aa.learner_id, aa.policy_id, aa.text AS arm, r.variable_id as reward_id, r.value as reward_value, r.timestamp as reward_time, aa.timestamp as arm_time
            FROM arm_assignments aa LEFT JOIN reward_values r ON aa.learner_id = r.learner_id and aa.timestamp < r.timestamp;
        """)
        # Second, find largest rewards for each LEFT join pair.
        cursor.execute("""
            CREATE TEMPORARY VIEW arm_reward_merged_max AS
            WITH arm_reward_merged_with_rank AS (
            SELECT
                *,
                ROW_NUMBER() OVER(PARTITION BY assignment_id ORDER BY reward_time) AS row_number
                FROM arm_reward_merged
            )
            SELECT
                *, COALESCE(arm_reward_merged_with_rank.reward_time, arm_reward_merged_with_rank.arm_time) AS time_to_find_contexts
                FROM arm_reward_merged_with_rank
                WHERE row_number = 1;
        """)
        # Manually add a column called time_to_find_contexts to use to find contexts.

        # Merge contexts with arm_reward_merged_max

        # Now, it has column: ['assignment_id', 'learner_id', 'policy_id', 'arm', 'reward_id', 'reward_value', 'reward_time', 'arm_time', 'row_number', 'time_to_find_contexts']

        # First, merge arm assignments with contexts before it.
        cursor.execute("""
            CREATE TEMPORARY VIEW contexts_merged AS
            SELECT ar.assignment_id, ar.learner_id, ar.policy_id, ar.arm, ar.reward_id, ar.reward_value, ar.reward_time, ar.arm_time, ar.row_number, ar.time_to_find_contexts, c.value as context_value, c.variable_id as context_variable_id, c.timestamp as context_time
            FROM arm_reward_merged_max ar LEFT JOIN context_values c ON ar.learner_id = c.learner_id and c.timestamp < ar.time_to_find_contexts;
        """)

        # cursor.execute("Select * FROM contexts_merged")
        # colnames = [desc[0] for desc in cursor.description]
        # print(colnames)

        # Second, find largest contexts for each LEFT join pair.
        cursor.execute("""
            CREATE TEMPORARY VIEW contexts_merged_max AS
            WITH arm_reward_contexts_merged_with_rank AS (
            SELECT
                *,
                ROW_NUMBER() OVER(PARTITION BY (assignment_id, context_variable_id) ORDER BY context_variable_id) AS row_number2
                FROM contexts_merged
            )
            SELECT
                *
                FROM arm_reward_contexts_merged_with_rank
                WHERE row_number2 = 1;
        """)


        cursor.execute("""
            SELECT t0.assignment_id, t0.learner_id, t3.name as policy_name, t0.arm, t0.arm_time, t1.name as reward_name, t0.reward_value, t2.name as context_name, t0.context_value, t0.context_time from contexts_merged_max t0 LEFT JOIN engine_variable t1 on (t0.reward_id = t1.id) LEFT JOIN engine_variable t2 on (t0.context_variable_id = t2.id) LEFT JOIN engine_policy t3 on (t0.policy_id = t3.id);
        """)

        result = cursor.fetchall()

        df = pd.DataFrame(data = result, columns= [i[0] for i in cursor.description])

        pivot_df = df.pivot(index=['assignment_id', 'learner_id', 'policy_name', 'arm', 'arm_time', 'reward_name', 'reward_value'],
                            columns='context_name',
                            values=['context_value', 'context_time'])

        # Flatten the column names
        pivot_df.columns = [f'{col[0]}_{col[1]}' for col in pivot_df.columns]

        pivot_df.replace({np.nan: None}, inplace = True)
        # Reset the index
        pivot_df = pivot_df.drop(['context_value_nan','context_time_nan', 'assignment_id'], axis=1, errors='ignore')
        pivot_df = pivot_df.reset_index()
        pivot_df = pivot_df.rename(columns={'policy_name': 'policy', 'reward_value': 'reward'})

        cursor.close()
        return pivot_df
    except Exception as e:
        # empty
        print(e)
        cursor.close()
        df = pd.DataFrame()
        return df

def data_downloader_helper(mooclet_name, reward_variable_name):
    cursor = conn.cursor()
    cursor.execute("select id from engine_mooclet where name = %s;", [mooclet_name]);
    mooclet_id = cursor.fetchone()[0]
    print("************************************************")
    cursor.execute("""
        -- Mooclets
        DROP TABLE IF EXISTS data_download_mooclet CASCADE;

        CREATE TABLE data_download_mooclet AS 
        TABLE "engine_mooclet" 
        WITH NO DATA;

        -- Variables
        DROP TABLE IF EXISTS data_download_variable CASCADE;

        CREATE TABLE data_download_variable AS 
        TABLE "engine_variable" 
        WITH NO DATA;

        -- Versions
        DROP TABLE IF EXISTS data_download_version CASCADE;

        CREATE TABLE data_download_version AS 
        TABLE "engine_version" 
        WITH NO DATA;

        -- Learners
        DROP TABLE IF EXISTS data_download_learner CASCADE;

        CREATE TABLE data_download_learner AS 
        TABLE "engine_learner" 
        WITH NO DATA;

        -- Policies
        DROP TABLE IF EXISTS data_download_policy CASCADE;

        CREATE TABLE data_download_policy AS 
        TABLE "engine_policy" 
        WITH NO DATA;

        -- Policy Parameters
        DROP TABLE IF EXISTS data_download_policyparameters CASCADE;

        CREATE TABLE data_download_policyparameters AS 
        TABLE "engine_policyparameters" 
        WITH NO DATA;

        -- Policy Parameters History
        DROP TABLE IF EXISTS data_download_policyparametershistory CASCADE;

        CREATE TABLE data_download_policyparametershistory AS 
        TABLE "engine_policyparametershistory" 
        WITH NO DATA;

        -- Values
        DROP TABLE IF EXISTS data_download_value CASCADE;

        CREATE TABLE data_download_value AS 
        TABLE "engine_value" 
        WITH NO DATA;

        -- Copy data from server
        DROP TABLE IF EXISTS variable_names CASCADE;
        DROP TABLE IF EXISTS context_names CASCADE;

        DROP VIEW IF EXISTS temp_mooclet CASCADE;
        DROP VIEW IF EXISTS temp_version CASCADE;
        DROP VIEW IF EXISTS temp_policy_parameters CASCADE;
        DROP VIEW IF EXISTS temp_policy_parameters_history CASCADE;
        DROP VIEW IF EXISTS temp_variable CASCADE;
        DROP VIEW IF EXISTS temp_policy CASCADE;
        DROP VIEW IF EXISTS temp_value CASCADE;
        DROP VIEW IF EXISTS temp_learner CASCADE;
        -- Get target mooclet given mooclet id or mooclet name
        CREATE VIEW temp_mooclet AS
        (WITH matching_id_mooclet AS (SELECT * FROM "engine_mooclet" WHERE id = %s) -- this is from the argument
        SELECT * FROM matching_id_mooclet 
        WHERE (SELECT COUNT(*) FROM matching_id_mooclet) = 1
        UNION
        SELECT * FROM "engine_mooclet"
        WHERE (SELECT COUNT(*) FROM matching_id_mooclet) = 0
        AND name = 'this is a dummy name'); -- this is from the argument

        -- Get all versions with the target mooclet
        CREATE VIEW temp_version AS
        (SELECT "engine_version".*
        FROM temp_mooclet, "engine_version"
        WHERE temp_mooclet.id = "engine_version".mooclet_id
        ORDER BY id ASC);

        -- Get current policy parameters with the target mooclet
        CREATE VIEW temp_policy_parameters AS
        (SELECT "engine_policyparameters".*
        FROM temp_mooclet, "engine_policyparameters"
        WHERE temp_mooclet.id = "engine_policyparameters".mooclet_id
        ORDER BY id ASC);

        -- Get history policy parameters with the target mooclet
        CREATE VIEW temp_policy_parameters_history AS
        (SELECT "engine_policyparametershistory".*
        FROM temp_mooclet, "engine_policyparametershistory"
        WHERE temp_mooclet.id = "engine_policyparametershistory".mooclet_id
        ORDER BY id ASC);

        -- Get all variables that is used in policy parameters, or given reward name or id
        CREATE TABLE context_names (
            name text
        );

        INSERT into context_names 
        select distinct(name) from (select * from engine_value where mooclet_id = %s) t1 JOIN engine_variable on (t1.variable_id = engine_variable.id  and engine_variable.name != 'version' and engine_variable.name != %s);


        CREATE TABLE variable_names AS
        (SELECT DISTINCT(parameters->>'outcome_variable') AS name FROM temp_policy_parameters
        UNION
        SELECT DISTINCT(parameters->>'outcome_variable_name') AS name FROM temp_policy_parameters
        UNION
        SELECT DISTINCT(parameters->>'outcome_variable') AS name FROM temp_policy_parameters_history
        UNION
        SELECT DISTINCT(parameters->>'outcome_variable_name') AS name FROM temp_policy_parameters_history);

        DELETE FROM context_names WHERE name IS NULL OR name = 'version';

        CREATE VIEW temp_variable AS
        (SELECT DISTINCT * 
        FROM "engine_variable" 
        WHERE "engine_variable".name = 'version'
            OR "engine_variable".name = %s -- this is from the argument
            OR "engine_variable".name IN (SELECT name FROM context_names)
        ORDER BY id ASC);

        INSERT INTO variable_names
        (SELECT temp_variable.name FROM temp_variable);

        DELETE FROM variable_names WHERE name IS NULL;

        -- Get all policies involved in the target mooclet
        CREATE VIEW temp_policy AS
        (WITH policies AS
        (SELECT policy_id as id FROM temp_policy_parameters
        UNION
        SELECT policy_id as id FROM temp_policy_parameters_history)
        SELECT DISTINCT("engine_policy".*)
        FROM policies, "engine_policy"
        WHERE policies.id = "engine_policy".id
        ORDER BY id ASC);

        -- Get all values with the target mooclet
        CREATE VIEW temp_value AS
        (SELECT "engine_value".*
        FROM temp_mooclet, "engine_value"
        WHERE temp_mooclet.id = "engine_value".mooclet_id
        ORDER BY id ASC);

        -- Get all learners involved in the target mooclet
        CREATE VIEW temp_learner AS
        (SELECT DISTINCT("engine_learner".*)
        FROM temp_value, "engine_learner"
        WHERE temp_value.learner_id IS NOT NULL
            AND temp_value.learner_id = "engine_learner".id
        ORDER BY id ASC);

        -- Insert to tables
        INSERT INTO data_download_mooclet
        (SELECT * FROM temp_mooclet);

        INSERT INTO data_download_version
        (SELECT * FROM temp_version);

        INSERT INTO data_download_policyparameters
        (SELECT * FROM temp_policy_parameters);

        INSERT INTO data_download_policyparametershistory
        (SELECT * FROM temp_policy_parameters_history);

        INSERT INTO data_download_variable
        (SELECT * FROM temp_variable);

        INSERT INTO data_download_policy
        (SELECT * FROM "engine_policy");

        INSERT INTO data_download_value
        (SELECT * FROM temp_value);

        INSERT INTO data_download_learner
        (SELECT * FROM temp_learner);

        -- SELECT column_name, data_type
        -- FROM information_schema.columns
        -- WHERE table_schema = 'public' AND 
        -- table_name = 'data_download_variable';

        DROP VIEW IF EXISTS arm_value CASCADE;
        DROP VIEW IF EXISTS context_value CASCADE;
        DROP VIEW IF EXISTS reward_value CASCADE;
        DROP VIEW IF EXISTS arm_reward_merged CASCADE;
        DROP VIEW IF EXISTS context_merged CASCADE;

        DROP TABLE IF EXISTS data_download_dataprocess CASCADE;

        -- Get arm value
        CREATE VIEW arm_value AS
        (WITH version_variable AS (SELECT id FROM data_download_variable WHERE name = 'version')
        SELECT 
            data_download_value.*,
            data_download_version.name AS arm_name
        FROM data_download_version, data_download_value, version_variable
        WHERE data_download_value.version_id IS NOT NULL
            AND data_download_value.variable_id IS NOT NULL
            AND data_download_version.id = data_download_value.version_id
            AND version_variable.id = data_download_value.variable_id
        ORDER BY data_download_value.timestamp ASC);

        -- Get context value
        CREATE VIEW context_value AS
        (WITH context_variable AS 
            (SELECT 
                data_download_variable.id AS id,
                data_download_variable.name AS name
            FROM data_download_variable, context_names 
            WHERE data_download_variable.name = context_names.name)
        SELECT 
            data_download_value.*,
            context_variable.name AS context_name
        FROM data_download_value, context_variable
        WHERE data_download_value.variable_id IS NOT NULL
            AND context_variable.id = data_download_value.variable_id
        ORDER BY data_download_value.timestamp ASC);

        -- Get reward value
        CREATE VIEW reward_value AS
        (WITH reward_variable AS
            (WITH reward_variable_names AS
                (SELECT name FROM variable_names WHERE name != 'version'
                EXCEPT
                SELECT name FROM context_names)
            SELECT 
                data_download_variable.id AS id,
                data_download_variable.name AS name
            FROM data_download_variable, reward_variable_names
            WHERE data_download_variable.name = reward_variable_names.name)
        SELECT 
            data_download_value.*,
            reward_variable.name AS reward_name
        FROM data_download_value, reward_variable
        WHERE data_download_value.variable_id IS NOT NULL
            AND reward_variable.id = data_download_value.variable_id
        ORDER BY data_download_value.timestamp ASC);

        -- (1) Merge arm value and reward value
        CREATE VIEW arm_reward_merged AS
        (SELECT DISTINCT * 
        FROM
            (SELECT 
                max(reward_var_id) OVER w2 AS reward_var_id,
                max(reward_name) OVER w2 AS reward_name,
                max(value) OVER w2 AS value,
                max(reward_text) OVER w2 AS reward_text,
                max(reward_create_time) OVER w2 AS reward_create_time,
                max(arm_var_id) OVER w2 AS arm_var_id,
                max(arm_name) OVER w2 AS arm_name,
                max(arm_text) OVER w2 AS arm_text,
                max(arm_assign_time) OVER w2 AS arm_assign_time,
                mooclet_id,
                learner_id,
                policy_id,
                version_id
            FROM
                (SELECT 
                    *, 
                    count(arm_var_id) OVER w1 AS grp
                FROM
                    (SELECT 
                        value, 
                        NULL::character varying (100) AS arm_name,
                        reward_name,
                        NULL::text AS arm_text,
                        text AS reward_text,
                        NULL::timestamp with time zone AS arm_assign_time,
                        timestamp AS reward_create_time,
                        learner_id,
                        mooclet_id,
                        policy_id,
                        version_id,
                        variable_id AS reward_var_id, 
                        NULL::integer AS arm_var_id,
                        timestamp
                    FROM reward_value
                    UNION ALL
                    SELECT 
                        NULL::double precision AS value, 
                        arm_name,
                        NULL::character varying (100) AS reward_name,
                        text AS arm_text,
                        NULL::text AS reward_text,
                        timestamp AS arm_assign_time,
                        NULL::timestamp with time zone AS reward_create_time,
                        learner_id,
                        mooclet_id,
                        policy_id,
                        version_id,
                        NULL::integer AS reward_var_id, 
                        variable_id AS arm_var_id,
                        timestamp
                    FROM arm_value
                    ) s1
                WINDOW w1 AS (PARTITION BY mooclet_id, version_id, policy_id, learner_id ORDER BY timestamp)
                ) s2
            WINDOW w2 AS (PARTITION BY mooclet_id, version_id, policy_id, learner_id, grp)) s3
        WHERE arm_assign_time is NOT NULL
        ORDER BY arm_assign_time);

        -- (2) Merge context value
        -- (2) Merge context value
        CREATE VIEW context_merged AS
        (WITH context_before_arm AS
            (SELECT 
                MAX(context_value.timestamp) AS timestamp, 
                MAX(context_value.context_name) AS context_name, 
                context_value.variable_id, 
                arm_reward_merged.arm_assign_time
            FROM context_value, arm_reward_merged
            WHERE context_value.learner_id = arm_reward_merged.learner_id
                AND context_value.mooclet_id = arm_reward_merged.mooclet_id
                AND context_value.timestamp < arm_reward_merged.arm_assign_time
            GROUP BY context_value.variable_id, arm_reward_merged.arm_assign_time)
        SELECT *
        FROM
            (SELECT 
                max(mooclet_id) AS mooclet_id,
                max(learner_id) AS learner_id,
                max(reward_var_id) AS reward_var_id,
                max(reward_name) AS reward_name,
                max(value) AS value,
                max(reward_text) AS reward_text,
                max(reward_create_time) AS reward_create_time,
                max(arm_var_id) AS arm_var_id,
                max(version_id) AS version_id,
                max(arm_name) AS arm_name,
                max(arm_text) AS arm_text,
                arm_assign_time,
                json_agg(
                    json_build_object(
                        'variable_id', variable_id, 
                        'name', context_name,
                        'value', context_value, 
                        'text', text, 
                        'timestamp', timestamp
                    )
                ) AS contexts,
                max(policy_id) AS policy_id
            FROM 
                (SELECT 
                    s3.value AS context_value,
                    s3.text,
                    s3.variable_id,
                    s3.timestamp,
                    s3.context_name,
                    arm_reward_merged.reward_var_id,
                    arm_reward_merged.reward_name,
                    arm_reward_merged.value,
                    arm_reward_merged.reward_text,
                    arm_reward_merged.reward_create_time,
                    arm_reward_merged.arm_var_id,
                    arm_reward_merged.arm_name,
                    arm_reward_merged.arm_text,
                    arm_reward_merged.arm_assign_time,
                    arm_reward_merged.mooclet_id,
                    arm_reward_merged.learner_id,
                    arm_reward_merged.policy_id,
                    arm_reward_merged.version_id
                FROM 
                    arm_reward_merged
                FULL OUTER JOIN
                    (SELECT 
                        context_value.*,
                        context_before_arm.arm_assign_time
                    FROM context_value, context_before_arm
                    WHERE context_value.timestamp = context_before_arm.timestamp
                    AND context_value.variable_id = context_before_arm.variable_id
                    ) s3
                ON arm_reward_merged.arm_assign_time = s3.arm_assign_time 
                ) s1
            GROUP BY arm_assign_time) s2);

        -- (4) Merge policy parameters
        -- (5) Insert to final table
        CREATE TABLE data_download_dataprocess AS 
        (WITH policy_param_history_after_arm AS
            (SELECT 
                MIN(data_download_policyparametershistory.creation_time) AS creation_time,  
                context_merged.arm_assign_time
            FROM context_merged, data_download_policyparametershistory
            WHERE data_download_policyparametershistory.policy_id = context_merged.policy_id
                AND data_download_policyparametershistory.mooclet_id = context_merged.mooclet_id
                AND data_download_policyparametershistory.creation_time > context_merged.arm_assign_time
            GROUP BY context_merged.arm_assign_time)
        SELECT 
            s5.mooclet_id,
            s5.mooclet_name,
            s4.learner_id,
            s4.learner_name,
            s3.reward_var_id,
            s3.reward_name,
            s3.value,
            s3.reward_text,
            s3.reward_create_time,
            s3.arm_var_id,
            s3.version_id,
            s3.arm_name,
            s3.arm_text,
            s3.arm_assign_time,
            s3.contexts,
            s6.policy_id,
            s6.policy_name,
            s3.parameters,
            s3.creation_time
        FROM
            (SELECT
                context_merged.*,
                data_download_policyparametershistory.parameters,
                data_download_policyparametershistory.creation_time
            FROM data_download_policyparametershistory, context_merged, policy_param_history_after_arm
            WHERE data_download_policyparametershistory.policy_id = context_merged.policy_id
                AND data_download_policyparametershistory.mooclet_id = context_merged.mooclet_id
                AND data_download_policyparametershistory.creation_time = policy_param_history_after_arm.creation_time
                AND context_merged.arm_assign_time = policy_param_history_after_arm.arm_assign_time
            UNION ALL
            SELECT 
                s2.*,
                data_download_policyparameters.parameters,
                data_download_policyparameters.latest_update AS creation_time
            FROM
                (SELECT 
                    context_merged.*
                FROM  
                    context_merged LEFT JOIN
                    (SELECT  
                        context_merged.arm_assign_time
                    FROM context_merged, data_download_policyparametershistory
                    WHERE data_download_policyparametershistory.policy_id = context_merged.policy_id
                        AND data_download_policyparametershistory.mooclet_id = context_merged.mooclet_id
                        AND data_download_policyparametershistory.creation_time > context_merged.arm_assign_time
                    GROUP BY context_merged.arm_assign_time) s1
                ON context_merged.arm_assign_time = s1.arm_assign_time
                WHERE s1.arm_assign_time IS NULL) s2 LEFT JOIN
                    data_download_policyparameters
                ON data_download_policyparameters.policy_id = s2.policy_id
            ORDER BY arm_assign_time) s3
        LEFT JOIN (SELECT id as learner_id, name as learner_name FROM data_download_learner) s4
        ON s3.learner_id = s4.learner_id
        LEFT JOIN (SELECT id as mooclet_id, name as mooclet_name FROM data_download_mooclet) s5
        ON s3.mooclet_id = s5.mooclet_id
        LEFT JOIN (SELECT id as policy_id, name as policy_name FROM data_download_policy) s6
        ON s3.policy_id = s6.policy_id);

        select * from data_download_dataprocess;
    """, [mooclet_id, mooclet_id, reward_variable_name, reward_variable_name])

    result = cursor.fetchall()

    if(len(result) == 0):
        return {
            "status_code": 404, 
            "message": "Dataset is empty."
        }
    
    
    cursor.close()

    df = pd.DataFrame(data = result, columns= [i[0] for i in cursor.description])

    result = []
    available_contextual_names = []
    reserved_variable_names = ['UR_or_TSCONTEXTUAL', 'coef_draw', 'precesion_draw', '']

    for i, row in df.iterrows():
        for context in row['contexts']:
            if context['name'] not in available_contextual_names and context['name'] != None and context['name'] not in reserved_variable_names:
                available_contextual_names.append(context['name'])

    for i, row in df.iterrows():

        row_formatted = {
                "learner_id": row['learner_id'], 
                "reward": row['value'], 
                "reward_time": row['reward_create_time'], 
                "arm": row['arm_name'], 
                "arm_time": row['arm_assign_time'], 
                'policy': row['policy_name'], 
                # 'parameters': row['parameters']
        }

        for available_contextual_name in available_contextual_names:
            is_added = False
            for context in row['contexts']:
                if context['name'] == available_contextual_name:
                    row_formatted[f'CONTEXTUAL_{available_contextual_name}'] = context['value'] 
                    row_formatted[f'CONTEXTUAL_{available_contextual_name}_time'] = context['timestamp']
                    is_added = True
                    break
            if not is_added:
                row_formatted[f'CONTEXTUAL_{available_contextual_name}'] = np.nan
                row_formatted[f'CONTEXTUAL_{available_contextual_name}_time'] = np.nan

        result.append(row_formatted)

    df = pd.DataFrame(result)
    df = df[df['learner_id'].notna()]
    df['learner_id'] = df['learner_id'].astype('int')


    df['arm_time'] = pd.to_datetime(df['arm_time'])
    df['arm_time'] = df['arm_time'].astype(object).where(df['arm_time'].notnull(), None)
    df['reward_time'] = pd.to_datetime(df['reward_time'])
    df['reward_time'] = df['reward_time'].astype(object).where(df['reward_time'].notnull(), None)
    for available_contextual_name in available_contextual_names:
        df[f'CONTEXTUAL_{available_contextual_name}_time'] = pd.to_datetime(df[f'CONTEXTUAL_{available_contextual_name}_time'])
        df[f'CONTEXTUAL_{available_contextual_name}_time'] = df[f'CONTEXTUAL_{available_contextual_name}_time'].astype(object).where(df[f'CONTEXTUAL_{available_contextual_name}_time'].notnull(), None) 
    return df



with open('list_of_mooclet_names.txt') as f:
    lines = f.readlines()
    mooclet_names = [line.rstrip('\n') for line in lines]

for mooclet_name in mooclet_names:
    reward_variable_name = None
    try:
        reward_variable_name = find_reward_variable(mooclet_name)
    except:
        reward_variable_name = 'dummy reward name'
    df = data_downloader_local_new(mooclet_name, reward_variable_name)
    df.to_csv(f'./datasets/{mooclet_name}.csv', index=False)