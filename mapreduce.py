import mysql.connector
from mrjob.job import MRJob

import azure_credentials as az

cnx = mysql.connector.connect(user=az.user,
                              password=az.azure_mysql_password,
                              host="fhtw-bdinf-wi21b080-azuredatabase-mysql.mysql.database.azure.com",
                              port=3306,
                              database="criminalsoccer",
                              ssl_ca="DigiCertGlobalRootCA.crt.pem",
                              ssl_disabled=False)

cursor = cnx.cursor()


class CountNationalities(MRJob):
    global cnx, cursor

    def mapper(self, _, nationality):
        # Emit a key-value pair with the nationality as the key and 1 as the value
        yield (nationality, 1)

    def reducer(self, nationality, counts):
        # Sum the counts for each nationality
        total_count = sum(counts)

        # Store the results in the MySQL table
        insert_query = """INSERT INTO nationalities (country, appearances)
                           VALUES (%s, %s)
                           ON DUPLICATE KEY UPDATE country=country"""
        cursor.execute(insert_query, (nationality, total_count))
        cnx.commit()


if __name__ == '__main__':
    CountNationalities.run()
    cursor.close()
    cnx.close()
