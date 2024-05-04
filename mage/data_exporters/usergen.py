if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


from faker import Faker
import psycopg2
import random
fake = Faker()


# Generate user data
def gen_user_data(num_user_records: int) -> None:
    for id in range(num_user_records):
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="postgres",
            host="postgres",
        )
        curr = conn.cursor()
        curr.execute(
            """INSERT INTO commerce.users
             (id, username, password) VALUES (%s, %s, %s)""",
            (id, fake.user_name(), fake.password()),
        )
        curr.execute(
            """INSERT INTO commerce.products
             (id, name, description, price) VALUES (%s, %s, %s, %s)""",
            (id, fake.name(), fake.text(), fake.random_int(min=1, max=100)),
        )
        conn.commit()

        # update 10 % of the time
        if random.randint(1, 100) >= 90:
            curr.execute(
                "UPDATE commerce.users SET username = %s WHERE id = %s",
                (fake.user_name(), id),
            )
            curr.execute(
                "UPDATE commerce.products SET name = %s WHERE id = %s",
                (fake.name(), id),
            )
        conn.commit()
        curr.close()
    return

@data_exporter
def usergen(data,*args,**kwargs):
    num_user_records = data[2]
    _=gen_user_data(num_user_records)



