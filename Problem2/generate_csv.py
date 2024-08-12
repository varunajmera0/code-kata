import csv
import random
import faker


def generate_large_csv(file_path, num_rows):
    fake = faker.Faker()
    with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['first_name', 'last_name', 'address', 'date_of_birth']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for _ in range(num_rows):
            writer.writerow({
                'first_name': fake.first_name(),
                'last_name': fake.last_name(),
                'address': fake.address().replace("\n", ", "),
                'date_of_birth': fake.date_of_birth().isoformat()
            })


# Generate a 2GB file (e.g., around 20 million rows)
generate_large_csv('large_dataset.csv', 20000000)
