import requests
import json
import datetime

load_year = '23/24' #'all'

spr_sports = {1: 'Футбол', 4: 'Хоккей'}

sports = []
countrys = []
championships = []
tournaments = []
matches = []
teams = []
coefs = []

spr_country_top = [
    {'id': 826, 'sId': 4, 'catId': 65}, # Хоккей-Австрия-Национальная Лига
    {'id': 1370, 'sId': 4, 'catId': 275}, # Хоккей-Беларусь-Экстралига
    {'id': 225, 'sId': 4, 'catId': 41}, # Хоккей-Германия-DEL
    {'id': 267, 'sId': 4, 'catId': 41}, # Хоккей-Германия-DEL2
    {'id': 257, 'sId': 4, 'catId': 64}, # Хоккей-Дания-Метал Лиген
    {'id': 30156, 'sId': 4, 'catId': 810}, # Хоккей-Казахстан-Чемпионат
    {'id': 1454, 'sId': 4, 'catId': 176}, # Хоккей-Канада-OHL
    {'id': 30398, 'sId': 4, 'catId': 176}, # Хоккей-Канада-QMJHL
    {'id': 14361, 'sId': 4, 'catId': 176}, # Хоккей-Канада-WHL
    {'id': 19452, 'sId': 4, 'catId': 306}, # Хоккей-Латвия-Чемпионат
    {'id': 828, 'sId': 4, 'catId': 38}, # Хоккей-Норвегия-1-й дивизион
    {'id': 260, 'sId': 4, 'catId': 38}, # Хоккей-Норвегия-Фьордкрафт Лиген
    {'id': 243, 'sId': 4, 'catId': 115}, # Хоккей-Польша-PHL
    {'id': 1141, 'sId': 4, 'catId': 101}, # Хоккей-Россия-ВХЛ
    {'id': 268, 'sId': 4, 'catId': 101}, # Хоккей-Россия-КХЛ
    {'id': 1159, 'sId': 4, 'catId': 101}, # Хоккей-Россия-МХЛ
    {'id': 1490, 'sId': 4, 'catId': 98}, # Хоккей-Словакия-1-я лига
    {'id': 236, 'sId': 4, 'catId': 98}, # Хоккей-Словакия-Экстралига
    {'id': 844, 'sId': 4, 'catId': 37}, # Хоккей-США-AHL
    {'id': 234, 'sId': 4, 'catId': 37}, # Хоккей-США-НХЛ
    {'id': 259, 'sId': 4, 'catId': 40}, # Хоккей-Финляндия-Местис
    {'id': 134, 'sId': 4, 'catId': 40}, # Хоккей-Финляндия-Лиига
    {'id': 599, 'sId': 4, 'catId': 242}, # Хоккей-Франция-Лига Магнуса
    {'id': 735, 'sId': 4, 'catId': 42}, # Хоккей-Чехия-Ченс лига
    {'id': 237, 'sId': 4, 'catId': 42}, # Хоккей-Чехия-Экстралига
    {'id': 128, 'sId': 4, 'catId': 54}, # Хоккей-Швейцария-Национальная лига
    {'id': 129, 'sId': 4, 'catId': 54}, # Хоккей-Швейцария-Суисс лига
    {'id': 416, 'sId': 4, 'catId': 39}, # Хоккей-Швеция-Аллсвенскан
    {'id': 261, 'sId': 4, 'catId': 39}, # Хоккей-Швеция-SHL
    {'id': 714, 'sId': 4, 'catId': 39}, # Хоккей-Швеция-АллЭттан
    {'id': 171, 'sId': 1, 'catId': 102}, # Футбол-Кипр-Чемпионат
    {'id': 649, 'sId': 1, 'catId': 99}, # Футбол-Китай-Чемпионат
    {'id': 27070, 'sId': 1, 'catId': 274}, # Футбол-Колумбия-Чемпионат
    {'id': 14189, 'sId': 1, 'catId': 565}, # Футбол-Косово-Суперлига
    {'id': 1002, 'sId': 1, 'catId': 331}, # Футбол-Кувейт-Премьер Лига
    {'id': 197, 'sId': 1, 'catId': 163}, # Футбол-Латвия-Чемпионат
    {'id': 2386, 'sId': 1, 'catId': 428}, # Футбол-Ливан-Премьер Лига
    {'id': 198, 'sId': 1, 'catId': 160}, # Футбол-Литва-Чемпионат
    {'id': 690, 'sId': 1, 'catId': 197}, # Футбол-Люксембург-Национальный дивизион
    {'id': 19226, 'sId': 1, 'catId': 85}, # Футбол-Малайзия-Премьер Лига
    {'id': 1000, 'sId': 1, 'catId': 85}, # Футбол-Малайзия-Суперлига
    {'id': 629, 'sId': 1, 'catId': 134}, # Футбол-Мальта-Чемпионат
    {'id': 937, 'sId': 1, 'catId': 303}, # Футбол-Марокко-Чемпионат
    {'id': 685, 'sId': 1, 'catId': 279}, # Футбол-Молдова-Чемпионат
    {'id': 2112, 'sId': 1, 'catId': 532}, # Футбол-Нигерия-Премьер-Лига
    {'id': 19242, 'sId': 1, 'catId': 389}, # Футбол-Никарагуа-Примера Дивизион
    {'id': 594, 'sId': 1, 'catId': 148}, # Футбол-Новая Зеландия-Чемпионат
    {'id': 22, 'sId': 1, 'catId': 5}, # Футбол-Норвегия-1-й дивизион
    {'id': 20, 'sId': 1, 'catId': 5}, # Футбол-Норвегия-Чемпионат
    {'id': 971, 'sId': 1, 'catId': 299}, # Футбол-Объединенные Арабские Эмираты-Чемпионат
    {'id': 965, 'sId': 1, 'catId': 415}, # Футбол-Оман-Чемпионат
    {'id': 406, 'sId': 1, 'catId': 20}, # Футбол-Перу-Чемпионат
    {'id': 202, 'sId': 1, 'catId': 47}, # Футбол-Польша-Чемпионат
    {'id': 239, 'sId': 1, 'catId': 44}, # Футбол-Португалия-2-й дивизион
    {'id': 238, 'sId': 1, 'catId': 44}, # Футбол-Португалия-Чемпионат
    {'id': 410, 'sId': 1, 'catId': 291}, # Футбол-Республика Корея-Чемпионат
    {'id': 879, 'sId': 1, 'catId': 21}, # Футбол-Россия-Турнир молодежных команд
    {'id': 203, 'sId': 1, 'catId': 21}, # Футбол-Россия-Премьер Лига
    {'id': 204, 'sId': 1, 'catId': 21}, # Футбол-Россия-1-я лига
    {'id': 20162, 'sId': 1, 'catId': 951}, # Футбол-Руанда-Премьер Лига
    {'id': 152, 'sId': 1, 'catId': 77}, # Футбол-Румыния-Чемпионат
    {'id': 955, 'sId': 1, 'catId': 310}, # Футбол-Саудовская аравия-Чемпионат
    {'id': 200, 'sId': 1, 'catId': 130}, # Футбол-Северная Ирландия-Премьер-лига
    {'id': 701, 'sId': 1, 'catId': 130}, # Футбол-Северная Ирландия-Чемпионшип
    {'id': 1226, 'sId': 1, 'catId': 886}, # Футбол-Сенегал-Лига 1
    {'id': 210, 'sId': 1, 'catId': 152}, # Футбол-Сербия-Чемпионат
    {'id': 634, 'sId': 1, 'catId': 45}, # Футбол-Сингапур-Чемпионат
    {'id': 211, 'sId': 1, 'catId': 23}, # Футбол-Словакия-Чемпионат
    {'id': 212, 'sId': 1, 'catId': 24}, # Футбол-Словения-Чемпионат
    {'id': 242, 'sId': 1, 'catId': 26}, # Футбол-США-MLS
    {'id': 1032, 'sId': 1, 'catId': 485}, # Футбол-Таиланд-Чемпионат
    {'id': 984, 'sId': 1, 'catId': 378}, # Футбол-Тунис-Чемпионат
    {'id': 98, 'sId': 1, 'catId': 46}, # Футбол-Турция-1-я Лига
    {'id': 14864, 'sId': 1, 'catId': 824}, # Футбол-Уганда-Премьер-лига
    {'id': 772, 'sId': 1, 'catId': 385}, # Футбол-Узбекистан-Суперлига
    {'id': 892, 'sId': 1, 'catId': 86}, # Футбол-Украина-Молодежная лига
    {'id': 218, 'sId': 1, 'catId': 86}, # Футбол-Украина-Чемпионат
    {'id': 1908, 'sId': 1, 'catId': 57}, # Футбол-Уругвай-Сегунда дивизион
    {'id': 278, 'sId': 1, 'catId': 57}, # Футбол-Уругвай-Чемпионат
    {'id': 254, 'sId': 1, 'catId': 131}, # Футбол-Уэльс-Премьер Лига
    {'id': 673, 'sId': 1, 'catId': 201}, # Футбол-Фарерские острова-Чемпионат
    {'id': 1654, 'sId': 1, 'catId': 847}, # Футбол-Филиппины-Чемпионат
    {'id': 41, 'sId': 1, 'catId': 19}, # Футбол-Финляндия-Чемпионат
    {'id': 55, 'sId': 1, 'catId': 19}, # Футбол-Финляндия-2-й дивизион
    {'id': 170, 'sId': 1, 'catId': 14}, # Футбол-Хорватия-Чемпионат
    {'id': 724, 'sId': 1, 'catId': 14}, # Футбол-Хорватия-2-й дивизион
    {'id': 717, 'sId': 1, 'catId': 386}, # Футбол-Черногория-2-й дивизион
    {'id': 154, 'sId': 1, 'catId': 386}, # Футбол-Черногория-Чемпионат
    {'id': 172, 'sId': 1, 'catId': 18}, # Футбол-Чехия-Чемпионат
    {'id': 27665, 'sId': 1, 'catId': 49}, # Футбол-Чили-Чемпионат
    {'id': 215, 'sId': 1, 'catId': 25}, # Футбол-Швейцария-Чемпионат
    {'id': 216, 'sId': 1, 'catId': 25}, # Футбол-Швейцария-1-й дивизион
    {'id': 40, 'sId': 1, 'catId': 9}, # Футбол-Швеция-Чемпионат
    {'id': 46, 'sId': 1, 'catId': 9}, # Футбол-Швеция-Суперэттан
    {'id': 207, 'sId': 1, 'catId': 22}, # Футбол-Шотландия-1-я Лига
    {'id': 36, 'sId': 1, 'catId': 22}, # Футбол-Шотландия-Премьер-Лига
    {'id': 206, 'sId': 1, 'catId': 22}, # Футбол-Шотландия-Чемпионшип
    {'id': 178, 'sId': 1, 'catId': 92}, # Футбол-Эстония-Чемпионат
    {'id': 358, 'sId': 1, 'catId': 322}, # Футбол-Южная Африка-Чемпионат
    {'id': 1892, 'sId': 1, 'catId': 502}, # Футбол-Ямайка-Премьер-лига
    {'id': 196, 'sId': 1, 'catId': 52}, # Футбол-Япония-Чемпионат
    {'id': 33656, 'sId': 1, 'catId': 914}, # Футбол-Эфиопия-Премьер-лига
    {'id': 33980, 'sId': 1, 'catId': 379}, # Футбол-Боливия-Чемпионат
    {'id': 27072, 'sId': 1, 'catId': 274}, # Футбол-Колумбия-Чемпионат
    {'id': 1900, 'sId': 1, 'catId': 352}, # Футбол-Индия-Суперлига
    {'id': 1015, 'sId': 1, 'catId': 368}, # Футбол-Индонезия-Лига 1
    {'id': 929, 'sId': 1, 'catId': 329}, # Футбол-Иордания-Чемпионат
    {'id': 915, 'sId': 1, 'catId': 301}, # Футбол-Иран-Чемпионат
    {'id': 193, 'sId': 1, 'catId': 51}, # Футбол-Ирландия-1-й дивизион
    {'id': 192, 'sId': 1, 'catId': 51}, # Футбол-Ирландия-Чемпионат
    {'id': 675, 'sId': 1, 'catId': 10}, # Футбол-Исландия-1-й дивизион
    {'id': 188, 'sId': 1, 'catId': 10}, # Футбол-Исландия-Чемпионат
    {'id': 785, 'sId': 1, 'catId': 278}, # Футбол-Казахстан-1-й дивизион
    {'id': 682, 'sId': 1, 'catId': 278}, # Футбол-Казахстан-Чемпионат
    {'id': 19250, 'sId': 1, 'catId': 852}, # Футбол-Камбоджа-С-Лига
    {'id': 1006, 'sId': 1, 'catId': 391}, # Футбол-Камерун-Чемпионат
    {'id': 28432, 'sId': 1, 'catId': 388}, # Футбол-Канада-Премьер Лига
    {'id': 482, 'sId': 1, 'catId': 388}, # Футбол-Канада-CSL
    {'id': 825, 'sId': 1, 'catId': 353}, # Футбол-Катар-Чемпионат
    {'id': 1644, 'sId': 1, 'catId': 805}, # Футбол-Кения-Премьер Лига
    {'id': 727, 'sId': 1, 'catId': 66}, # Футбол-Израиль-2-й дивизион
    {'id': 266, 'sId': 1, 'catId': 66}, # Футбол-Израиль-Премьер Лигаl
    {'id': 848, 'sId': 1, 'catId': 352}, # Футбол-Индия-Чемпионат
    {'id': 808, 'sId': 1, 'catId': 305}, # Футбол-Египет-Чемпионат
    {'id': 65, 'sId': 1, 'catId': 8}, # Футбол-Дания-2-й дивизион
    {'id': 47, 'sId': 1, 'catId': 8}, # Футбол-Дания-1-й дивизион
    {'id': 704, 'sId': 1, 'catId': 270}, # Футбол-Грузия-Национальная лига
    {'id': 185, 'sId': 1, 'catId': 67}, # Футбол-Греция-Чемпионат
    {'id': 947, 'sId': 1, 'catId': 339}, # Футбол-Гонконг, Китай-Премьер-лига
    {'id': 37, 'sId': 1, 'catId': 35}, # Футбол-Голландия (Нидерланды)-Чемпионат
    {'id': 131, 'sId': 1, 'catId': 35}, # Футбол-Голландия (Нидерланды)-1-й дивизион
    {'id': 1191, 'sId': 1, 'catId': 542}, # Футбол-Гана-Премьер-лига
    {'id': 231, 'sId': 1, 'catId': 281}, # Футбол-Венесуэла-Чемпионат
    {'id': 187, 'sId': 1, 'catId': 11}, # Футбол-Венгрия-Чемпионат
    {'id': 325, 'sId': 1, 'catId': 13}, # Футбол-Бразилия-Серия A'
    {'id': 390, 'sId': 1, 'catId': 13}, # Футбол-Бразилия-Ceрия B
    {'id': 92, 'sId': 1, 'catId': 13}, # Футбол-Бразилия-Кариока
    {'id': 382, 'sId': 1, 'catId': 13}, # Футбол-Бразилия-Паранаэнсе
    {'id': 247, 'sId': 1, 'catId': 78}, # Футбол-Болгария-Чемпионат
    {'id': 38, 'sId': 1, 'catId': 33}, # Футбол-Бельгия-Чемпионат
    {'id': 169, 'sId': 1, 'catId': 91}, # Футбол-Беларусь-Чемпионат
    {'id': 671, 'sId': 1, 'catId': 296}, # Футбол-Армения-Чемпионат
    {'id': 155, 'sId': 1, 'catId': 48}, # Футбол-Аргентина-Чемпионат
    {'id': 841, 'sId': 1, 'catId': 304}, # Футбол-Алжир-Чемпионат
    {'id': 720, 'sId': 1, 'catId': 257}, # Футбол-Албания-Чемпионат
    {'id': 709, 'sId': 1, 'catId': 297}, # Футбол-Азербайджан-Чемпионат
    {'id': 136, 'sId': 1, 'catId': 34}, # Футбол-Австралия-А-Лига
    {'id': 182, 'sId': 1, 'catId': 7},  # Футбол-Франция-Лига 2
    {'id': 34, 'sId': 1, 'catId': 7},  # Футбол-Франция-Лига 1
    {'id': 53, 'sId': 1, 'catId': 31}, # Футбол-Италия-Серия B
    {'id': 23, 'sId': 1, 'catId': 31}, # Футбол-Италия-Серия A
    {'id': 35, 'sId': 1, 'catId': 30}, # Футбол-Германия-Бундеслига
    {'id': 44, 'sId': 1, 'catId': 30}, # Футбол-Германия-2-я Бундеслига
    {'id': 491, 'sId': 1, 'catId': 30}, # Футбол-Германия-3-я Бундеслига
    {'id': 8, 'sId': 1, 'catId': 32}, # Футбол-Испания-Ла Лига
    {'id': 54, 'sId': 1, 'catId': 32}, # Футбол-Испания-Ла Лига 2
    {'id': 173, 'sId': 1, 'catId': 1}, # Футбол-Англия-Национальная лига
    {'id': 17, 'sId': 1, 'catId': 1}, # Футбол-Англия-Премьер-Лига'
    {'id': 18, 'sId': 1, 'catId': 1}, # Футбол-Англия-Чемпионшип
    {'id': 24, 'sId': 1, 'catId': 1},  # Футбол-Англия-1-я Лига
    {'id': 25, 'sId': 1, 'catId': 1}, # Футбол-Англия-2-я Лига
    {'id': 203, 'sId': 1, 'catId': 21}, # Футбол-РФ-Премьер Лига
    {'id': 879, 'sId': 1, 'catId': 21}, # Футбол-РФ-Турнир молодежных команд
    {'id': 204, 'sId': 1, 'catId': 21}, # Футбол-РФ-1-я лига
]

def is_country_top(id):
    for dat in spr_country_top:
        if dat['catId'] == id:
            return True
    return False

def is_liga_top(id):
    for dat in spr_country_top:
        if dat['id'] == id:
            return True
    return False

def get_sport(id):
    for sport in sports:
        if id == sport['id']:
            return sport['name']
    return 'Вид спорта нре найден'

def get_country(id):
    for country in countrys:
        if id == country['id']:
            return country['name']
    return 'Страна не найдена'

def get_tournament(id):
    for tournament in tournaments:
        if id == tournament['id']:
            return tournament['name']
    return 'Турнир не найден'

def get_championship(id):
    for championship in championships:
        if id == championship['id']:
            return championship['name']
    return 'Чемпионат не найден'

def get_team(id):
    for team in teams:
        if id == team['id']:
            return team['name']
    return 'Команда не найдена'

url_categories = 'https://stat-api.baltbet.ru/api/categories'
url_countrys = 'https://stat-api.baltbet.ru/api/sports/%s/tournaments'
url_tournaments = 'https://stat-api.baltbet.ru/api/tournaments/%s/seasons'
url_matches = 'https://stat-api.baltbet.ru/api/seasons/%s/matches'
url_coefs = 'https://stat-api.baltbet.ru/api/coefs/%s'

response = requests.get(url_categories)
page_json = json.loads(response.text)
for data in page_json:
    if data['sport']['id'] in spr_sports:
        sports.append(data['sport'])

for sport in sports:
    url_sport = url_countrys % sport['id']
    response = requests.get(url_sport)
    page_json_sport = json.loads(response.text)
    print(f"Загрузка вида спорта: {get_sport(sport['id'])}->{url_sport}")
    for category in page_json_sport['categories']:
        if is_country_top(category['id']):
            countrys.append(category)
    for i in range(len(page_json_sport['tournaments'])):
        try:
            if is_liga_top(page_json_sport['tournaments'][i]['id']):
                championships.append(page_json_sport['tournaments'][i])
        except IndexError as err:
            print(f'ERROR!!! {err}')

# print(championships)
# exit()

for champion in championships:
    # Обрабатываем только Англию...
    if champion['catId'] != 1:
        continue
    try:
        url_tour = url_tournaments % champion['id']
        response = requests.get(url_tour)
        page_json_tourn = json.loads(response.text)

        print(f"Загрузка турнира: {get_sport(champion['sId'])}, {get_country(champion['catId'])} {get_championship(champion['id'])}->{url_tour}")

        for i in range(len(page_json_tourn)):
            tournaments.append(page_json_tourn[i])

    except:
        print(f"ERROR: id={champion['id']} catId={champion['catId']} name={champion['name']} url={url_tour}")

# print(tournaments)
# exit()

for tour in tournaments:
    try:
        if load_year != tour['year']:
            continue
        url_match = url_matches % tour['id']
        response = requests.get(url_match)
        page_json_matches = json.loads(response.text)

        print(f"Загрука матчей: {get_sport(tour['sId'])} {get_tournament(tour['id'])} {url_match}")

        for i in range(len(page_json_matches['matches'])):
            matches.append(page_json_matches['matches'][i])

        for i in range(len(page_json_matches['teams'])):
            teams.append(page_json_matches['teams'][i])

    except:
        print(f'ERROR: url={url_match}')

print(matches)
exit()

for match in matches:
    try:
        url_coef = url_coefs % match['id']
        response = requests.get(url_coef)
        page_json_coefs = json.loads(response.text)

        dt_object = datetime.datetime.fromtimestamp(match['time'])
        print(f"Загрука коэфф.: {get_sport(match['sId'])}, {get_tournament(tour['id'])}, {dt_object.strftime('%d.%m.%Y %H:%M')} {get_team(match['homeId'])}-{get_team(match['awayId'])} {url_coef}")

        for i in range(len(page_json_coefs['main'])):
            coefs.append(page_json_coefs['main'][i])

    except:
        print(f'ERROR: url={url_coef}')

print('Виды спорта:', len(sports))
print('Страны:', len(countrys))
print('Чемпионаты:', len(championships))
print('Турниры:', len(tournaments))
print('Матчи:', len(matches))
print('Команды:', len(teams))
print('Коэффициенты:', len(coefs))

exit()
