import csv

def read_spec(file_path):
    spec = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            field_name, length = line.strip().split(':')
            spec.append((field_name, int(length)))
    return spec

def parse_fixed_width(file_path, spec):
    parsed_data = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            record = []
            start = 0
            for field_name, length in spec:
                record.append(line[start:start+length].strip())
                start += length
            parsed_data.append(record)
    return parsed_data

def write_csv(file_path, data, headers):
    with open(file_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

def main():
    spec = read_spec('spec.txt')
    headers = [field_name for field_name, length in spec]
    parsed_data = parse_fixed_width('data.txt', spec)
    write_csv('./output.csv', parsed_data, headers)

if __name__ == '__main__':
    main()
