import concurrent
import traceback
from concurrent.futures import ThreadPoolExecutor

import openai
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from tqdm import tqdm

df = pd.read_excel("topics.xlsx")  # 

openai.api_key = "sk-vc****i"  # Ставим свой ключ
creds = Credentials.from_service_account_file('***.json')  # Кидаем api-ключ google в папку со скриптом и заполняем имя файла
folder_id = '1kXPbraZPgTzW2zNKGNDOKn2-KUkuGtJQ'  # ID папки в Google Docs

model = "gpt-3.5-turbo"  # Ставим нужную модель

service = build('docs', 'v1', credentials=creds)
drive_service = build('drive', 'v3', credentials=creds)

topics_list = []
links_list = []


def set_permissions(file_id):
    permissions = {
        'role': 'writer',
        'type': 'user'
    }
    drive_service.permissions().create(
        fileId=file_id,
        body=permissions,
        fields='id'
    ).execute()


def process_topic(topic):
    try:
        response = openai.ChatCompletion.create(
            model=model,
            messages=[
                {
                    "role": "assistant",
                    "content": f"Представь, что ты редактор спортивного СМИ, который раньше работал "
                               f"реабилитологом в сердечно-сосудистой хирургии. Придумай  структуру минимум из 7 заголовков h2. Тема нашей "
                               f"информационной статьи «{topic}». Статья должна быть оптимизированной под поисковую систему Google. "
                               f"Если после двоеточия перечислено не все возможные заголовки, дополни их, как будто ты "
                               f"эксперт в области фитнеса и здоровья — нам нужна полная структура статьи. Перечисления "
                               f"после двоеточия не обязательно являются отдельными заголовками — формируй структуру исходя из логики. "
                               f"Не нумеруй заголовки цифрами, просто отметь их «H2: » в начале заголовка."
                },
                {
                    "role": "user",
                    "content": "Для каждого заголовка h2 напиши по 2 идеи для содержания в формате вопроса. Сами "
                               "заголовки не обязательно делать вопросами."
                }
            ]
        )

        answer_gpt = response.choices[0].message.content

        body = {
            'title': topic
        }
        doc = service.documents() \
            .create(body=body).execute()

        try:
            second_topic = topic.split(":")[0]
        except IndexError:
            second_topic = topic

        pattern = f"Title: {topic}\n\nСодержание текста\n{answer_gpt}\nТехнические требования\nОбъем текста от X слов. Объем может быть больше или меньше, главное – раскрыть тему полностью, но без воды.\nУникальность по text.ru от 85%.\nИзбегаем речевого мусора, канцеляритов и вводных слов.\nПримеры текстов"

        requests = [
            {
                'insertText': {
                    'location': {'index': 1},
                    'text': pattern
                }
            },
        ]

        headings = ["Содержание текста", "Технические требования", "Примеры текстов"]

        for heading in headings:
            start_index = pattern.index(heading)
            end_index = start_index + len(heading)
            requests.append({
                'updateParagraphStyle': {
                    'range': {
                        'startIndex': start_index + 1,
                        'endIndex': end_index + 1
                    },
                    'paragraphStyle': {
                        'namedStyleType': 'HEADING_2',
                    },
                    'fields': 'namedStyleType'
                }
            })

        start_index = pattern.index("Технические требования") + len("Технические требования ")
        end_index = pattern.index("Примеры текстов")

        requests.append({
            'createParagraphBullets': {
                'range': {
                    'startIndex': start_index + 1,
                    'endIndex': end_index + 1
                },
                'bulletPreset': 'BULLET_DISC_CIRCLE_SQUARE'
            }
        })

        start_index = pattern.index("Объем текста от X слов.") + len("Объем текста от ")
        end_index = start_index + 1
        requests.append({
            'updateTextStyle': {
                'range': {
                    'startIndex': start_index + 1,
                    'endIndex': end_index + 1,
                },
                'textStyle': {
                    'backgroundColor': {
                        'color': {
                            'rgbColor': {'red': 0.98, 'green': 0.97, 'blue': 0.55}
                        }
                    }
                },
                'fields': 'backgroundColor'
            }
        })

        start_index = pattern.index(topic)
        end_index = start_index + len(topic)
        requests.append({
            'updateTextStyle': {
                'range': {
                    'startIndex': start_index + 1,
                    'endIndex': end_index + 1,
                },
                'textStyle': {
                    'backgroundColor': {
                        'color': {
                            'rgbColor': {'red': 0.98, 'green': 0.97, 'blue': 0.55}
                        }
                    }
                },
                'fields': 'backgroundColor'
            }
        })

        result = service.documents().batchUpdate(documentId=doc['documentId'],
                                                 body={'requests': requests}).execute()
        file_id = doc['documentId']

        file = drive_service.files().get(fileId=file_id, fields='parents').execute();
        previous_parents = ",".join(file.get('parents'))

        file = drive_service.files().update(fileId=file_id,
                                            addParents=folder_id,
                                            removeParents=previous_parents,
                                            fields='id, parents').execute()

        link = f"https://docs.google.com/document/d/{file.get('id')}"
        return topic, link

    except Exception as e:
        traceback.print_exc()
        return topic, None


topics_list = []
links_list = []
error_count = 0

with tqdm(total=len(df["Темы"]), desc="Создание документов") as pbar:
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        futures = []
        for topic in df["Темы"]:
            future = executor.submit(process_topic, topic)
            future.add_done_callback(lambda p: pbar.update())
            futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            try:
                topic, link = future.result()
                if link is not None:
                    topics_list.append(topic)
                    links_list.append(link)
            except Exception as e:
                traceback.print_exc()
                error_count += 1
                if error_count == 3:
                    raise ValueError("Программа остановлена после 3х неудачных попыток запросов")

df_links = pd.DataFrame({'Тема статьи': topics_list, 'Ссылка': links_list})
df_links.to_excel('article_tasks_links.xlsx', index=False)
