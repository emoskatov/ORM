import sqlalchemy
from sqlalchemy.orm import sessionmaker
from os import getenv
from models import create_tables, Publisher, Book, Shop, Sale, Stock
import json


# Для наглядности и возможности составления запроса без определенных параметров была создана функция
def create_connection(sqlsystem='postgresql', login='postgres', password='postgres', host='localhost', port=5432,
                      db_name='ORM'):
    try:
        engine = sqlalchemy.create_engine(f'{sqlsystem}://{login}:{password}@{host}:{port}/{db_name}')
        print('Соединение установлено')
    except:
        print(f'Ошибка подключения')
    return engine


engine = create_connection(password=getenv(
    "DB_PASSWORD"))  # Подключение к БД, когда не передаем дополнительные параметры они прописаны по умолчанию, пароль для сохраности получаем из переменной окружения

create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

with open('tests_data.json', 'rt') as f:
    data = json.load(f)

for line in data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[line.get('model')]
    session.add(model(id=line.get('pk'), **line.get('fields')))
try:  # Для наглядности и исключения ошибок обрабатываем исключения
    session.commit()
    print("Данные успешно загружены")
except:
    print("Произошла ошибка при загрузке данных")


def get_publisher():
    name = input('Введите имя или id автора: ')
    match name.isnumeric():
        case True:
            results = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale) \
                .join(Publisher, Publisher.id == Book.id_publisher) \
                .join(Stock, Stock.id_book == Book.id) \
                .join(Shop, Shop.id == Stock.id_shop) \
                .join(Sale, Sale.id_stock == Stock.id) \
                .filter(Publisher.id == name).all()

            print_result(results=results)

        case False:
            results = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale) \
                .join(Publisher, Publisher.id == Book.id_publisher) \
                .join(Stock, Stock.id_book == Book.id) \
                .join(Shop, Shop.id == Stock.id_shop) \
                .join(Sale, Sale.id_stock == Stock.id) \
                .filter(Publisher.name == name).all()

            print_result(results=results)


def print_result(results):
    if len(results) == 0:
        print("По вашему запросу ничего не найдено")
    else:
        for book, shop, price, date in results:
            print(f'{book: <40} | {shop: <10} | {price: <10} | {date}')


session.close()

if __name__ == '__main__':
    get_publisher()
