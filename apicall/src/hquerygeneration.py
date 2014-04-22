__author__ = 'rahman'

class HQueryGeneration:
    'generates all hive queries based on provided codes by WorkFlowEngine'

    def __init__(self):
        self.query = []

    # generate hive query for each request from ONav tool
    def generateQuery(self,qdata):

        user = ''
        cohort = ''
        medcodes = []

        length = len(qdata)

        for x in range(0,length-1):
            user = qdata["user"][x]
            cohort = qdata["cohort"][x]
            medcodes = qdata["codes"][x]

            #table = "%s.combids" % (user)
            table = "%s_combids" % (user)

            code_list = {}

            if not medcodes:
                return False

            medcodes.sort()
            last_fc = medcodes[0][0]
            code_list[last_fc] = []

            for code in medcodes:
                if code[0] == last_fc:
                    code_list[last_fc].append("medcode LIKE '%s__'" % code)
            else:
                last_fc = code[0]
                code_list[last_fc] = ["medcode LIKE '%s__'" % code]

            master_list = ''
            for key,value in code_list.iteritems():
                code_list[key] = "(medcode1 = '%s' AND (%s))" % (key, " OR ".join(value))

            master_list = " OR ".join(code_list.values())

            hive_tables = ['demography_norm', 'ahd_denorm', 'medical_denorm', 'pvi_norm', 'therapy_norm', 'consult_denorm', 'patient_norm']

            thin_tables = ['demography', 'ahd', 'medical', 'pvi', 'therapy', 'consult', 'patient']

            #queries for hive
            create_db = "CREATE DATABASE IF NOT EXISTS %s" % user
            working_db = "USE default"
            drop_table1 = "DROP TABLE IF EXISTS %s" % table
            create_table1 = "CREATE TABLE IF NOT EXISTS %s (combid STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" % table
            testing_codes = "INSERT OVERWRITE TABLE %s SELECT DISTINCT combid FROM medical_norm WHERE %s" % (table,master_list)
            drop_table2 = "DROP TABLE IF EXISTS %s_%s" % (user,hive_tables)
            create_table2 = "CRETAE TABLE %s_%s ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' AS SELECT * FROM %s LEFT SEMI JOIN %s ON (combids.combid = %s.combid)" % (user, hive_tables, thin_tables, table, thin_tables)

            self.query.append({'createdb':create_db, 'workdb':working_db, 'droptable1': drop_table1, 'createtable1': create_table1, 'testcode':testing_codes, 'droptable2':drop_table2, 'createtable2':create_table2})

        return self.query