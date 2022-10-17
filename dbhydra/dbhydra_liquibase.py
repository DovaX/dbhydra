# Auto generates XML files for liquibase for DORM tables

import dbhydra as dh


def generate_xml(file):
    with open(file, "a+") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<databaseChangeLog\n')
        f.write('xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n')
        f.write('xmlns:ext="http://www.liquibase.org/xml/ns/dbchangelog-ext"\n')
        f.write('xmlns="http://www.liquibase.org/xml/ns/dbchangelog"\n')
        f.write(
            'xsi:schemaLocation="http://www.liquibase.org/xml/ns/dbchangelog '
            'http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.1.xsd\n')
        f.write('xmlns="http://www.liquibase.org/xml/ns/dbchangelog"\n')
        f.write(
            'http://www.liquibase.org/xml/ns/dbchangelog-ext '
            'http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-ext.xsd">\n')


def create_table(file):
    with open(file, "a+") as f:
        f.write('<changeSet author="dominik" id="20200210-1">\n')

        f.write('<createTable tableName="generic_companies">\n')
        f.write('<column autoIncrement="true" name="id" type="bigint">\n')
        f.write('    <constraints primaryKey="true"/>\n')
        f.write('</column>\n')

        f.write('<column name="dealer_user_id" type="int">\n')
        f.write('    <constraints nullable="false" foreignKeyName="fk_generic_company_user" references="users(id)"/>\n')
        f.write('</column>\n')

        f.write('<column name="country_id" type="int">\n')
        f.write('    <constraints foreignKeyName="fk_generic_company_country" references="countries(id)"/>\n')
        f.write('</column>\n')

        f.write('<column name="city" type="varchar(120)">\n')
        f.write('    <constraints nullable="false"/>\n')
        f.write('</column>\n')

        f.write('<column name="tin" type="varchar(50)">\n')
        f.write('    <constraints nullable="false"/>\n')
        f.write('</column>\n')

        f.write('<column name="name" type="varchar(80)">\n')
        f.write('    <constraints nullable="false"/>\n')
        f.write('</column>\n')

        f.write('</createTable>\n')

        f.write('<rollback>\n')
        f.write('    <dropTable tableName="generic_companies"/>\n')
        f.write('</rollback>\n')
        f.write('</changeSet>\n')

        f.write('</databaseChangeLog>\n')


generate_xml("text.txt")
create_table("text.txt")
