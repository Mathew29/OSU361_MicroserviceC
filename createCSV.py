import zmq
import csv


def createCSV():
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:5569")

    try:
        while True:
            if socket.poll(1000):

                data = socket.recv_json()
                print(data)

                csv_file_path = 'data.csv'
                with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
                    fieldnames = ['asin', 'name', 'price',
                                  'discount', 'record_date', 'url']
                    writer = csv.DictWriter(file, fieldnames=fieldnames)

                    writer.writeheader()

                    for item in data:
                        for metric in item['metrics']:
                            writer.writerow({
                                'name': item['name'],
                                'asin': item['asin'],
                                'price': metric['price'],
                                'discount': metric['discount'],
                                'record_date': metric['record_date'],
                                'url': item['url']
                            })

                print(
                    f'CSV file &quote;{csv_file_path}&quote; has been created successfully')
                with open(csv_file_path, 'r', encoding='utf-8') as file:
                    file_content = file.read()
                    socket.send(file_content.encode('utf-8'))
    except KeyboardInterrupt:
        print("CSV Server is shutting down")

    finally:
        socket.close()
        print("CSV socket closed")


if __name__ == "__main__":
    createCSV()
