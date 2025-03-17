import aiohttp
import asyncio
import requests
import logging
import json
import sqlite3
from typing import Optional, Dict, Any, List, Callable
from functools import wraps
import time
from getpass import getpass

# Настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("api.log"),  # Запись в файл
        # logging.StreamHandler(),  # Вывод в консоль
    ],
)
logger = logging.getLogger("School21API")

def log_request_response(func):
    """
    Декоратор для логирования запросов и ответов.
    """

    @wraps(func)
    async def wrapper(*args, **kwargs):
        # Логируем начало вызова метода
        logger.info(f"Вызов метода: {func.__name__}")
        logger.info(f"Аргументы: args={args}, kwargs={kwargs}")

        try:
            # Выполняем метод
            result = await func(*args, **kwargs)
            # Логируем успешный результат
            logger.info(f"Метод {func.__name__} завершён успешно. Результат: {result}")
            return result
        except Exception as e:
            # Логируем ошибку
            logger.error(f"Ошибка в методе {func.__name__}: {e}", exc_info=True)
            raise  # Пробрасываем исключение дальше

    return wrapper

def paginated_request(page_size: int = 1000, concurrency_limit: int = 10):
    """
    Декоратор для выполнения постраничных запросов и объединения результатов.
    Продолжает выполнять запросы, пока длина ответа не станет нулевой.
    Подстраивается под любую структуру ответа.

    :param page_size: Количество элементов на одной странице.
    :param concurrency_limit: Максимальное количество одновременных запросов.
    """

    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Убираем limit и offset из kwargs, так как они будут управляться декоратором
            limit = kwargs.pop("limit", page_size)
            offset = kwargs.pop("offset", 0)

            # Список для хранения всех результатов
            all_results = []

            # Семафор для ограничения количества одновременных запросов
            semaphore = asyncio.Semaphore(concurrency_limit)

            async def fetch_page(current_offset: int):
                async with semaphore:  # Ограничиваем количество одновременных запросов
                    result = await func(*args, **kwargs, limit=limit, offset=current_offset)
                    return result

            while True:
                # Создаем задачи для текущего блока запросов
                tasks = [fetch_page(offset + i * page_size) for i in range(concurrency_limit)]
                results = await asyncio.gather(*tasks)

                # Проверяем, есть ли пустые ответы в текущем блоке
                empty_responses_detected = False
                for result in results:
                    response_data = result
                    if isinstance(result, dict):
                        # Если ответ — это словарь, ищем ключ, который содержит список
                        for key, value in result.items():
                            if isinstance(value, (list, tuple)):
                                response_data = value
                                break

                    # Если ответ пустой, отмечаем это и прерываем цикл
                    if not len(response_data):
                        empty_responses_detected = True
                        break

                    # Добавляем результаты в общий список
                    if isinstance(result, dict):
                        if not all_results:
                            all_results = result.copy()
                        else:
                            for key, value in result.items():
                                if isinstance(value, (list, tuple)):
                                    all_results[key].extend(value)
                    else:
                        all_results.extend(response_data)

                # Если обнаружен пустой ответ, завершаем цикл
                if empty_responses_detected:
                    break

                # Увеличиваем offset для следующего блока запросов
                offset += concurrency_limit * page_size

            # Возвращаем объединённый результат
            return all_results

        return wrapper

    return decorator

def batch_async_requests(concurrency_limit: int = 10):
    """
    Декоратор для выполнения асинхронных запросов с поддержкой семафора.
    Принимает массив переменных, создаёт задачи для асинхронных запросов и объединяет результаты в один словарь.

    :param concurrency_limit: Максимальное количество одновременных запросов.
    """

    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(self, items: List[Any], *args, **kwargs):
            # Словарь для хранения всех результатов
            all_results = {}

            # Семафор для ограничения количества одновременных запросов
            semaphore = asyncio.Semaphore(concurrency_limit)

            async def fetch_item(item: Any):
                async with semaphore:  # Ограничиваем количество одновременных запросов
                    return {item : await func(self, item, *args, **kwargs)}

            # Создаем задачи для каждого элемента
            tasks = [fetch_item(item) for item in items]
            results = await asyncio.gather(*tasks)

            # Объединяем результаты в один словарь
            for result in results:
                if result:  # Пропускаем None (если были ошибки)
                    all_results.update(result)

            # Возвращаем объединённый результат
            return all_results

        return wrapper

    return decorator

class School21API:
    def __init__(
        self,
        auth_url: str = "https://auth.sberclass.ru/auth/realms/EduPowerKeycloak/protocol/openid-connect/token",
        base_url: str = "https://edu-api.21-school.ru/services/21-school/api",
        api_key: str = "",
    ):
        self.auth_url = auth_url
        self.base_url = base_url
        self.api_key = self._get_token()
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "x-edu-org-unit-id": "6bfe3c56-0211-4fe1-9e59-51616caac4dd",
        }
        self.session = None  # Сессия будет создана при первом запросе

    def _get_token(self):
        try:
            with open('token.json', 'r') as f:
                data = json.load(f)
            if int(time.time()) - data['creation_time'] < data['expires_in']:
                return data['access_token']
            else:
                raise
        except:
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
            }

            data = f'client_id=s21-open-api&username={input("Login: ")}&password={getpass("Password: ")}&grant_type=password'
            response = requests.post(self.auth_url, headers=headers, data=data)
            token = response.json()
            token['creation_time'] = int(time.time())
            with open('token.json', 'w') as f:
                json.dump(token, f, indent=4)
            return token['access_token']

    async def _ensure_session(self):
        """Создаёт сессию, если она ещё не создана."""

        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(headers=self.headers)

    async def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        max_retries: int = 1000,
    ):
        """
        Выполняет HTTP-запрос с повторными попытками в случае ошибок.
        """

        await self._ensure_session()
        url = f"{self.base_url}/{endpoint}"
        retries = 0

        while retries < max_retries:
            try:
                async with self.session.request(method, url, params=params) as response:
                    logger.info(f"Request to {url} returned status {response.status}")
                    if response.status == 429:  # Too Many Requests
                        retry_after = int(response.headers.get("Retry-After", 1))
                        await asyncio.sleep(retry_after)
                        retries += 1
                        continue
                    response.raise_for_status()  # Проверка на другие ошибки
                    return await response.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                retries += 1
                if retries >= max_retries:
                    raise Exception(f"Request failed after {max_retries} retries: {e}")
                await asyncio.sleep(1)  # Ожидание перед повторной попыткой
    
    @log_request_response
    async def get_sales(self):
        return await self._make_request("GET", "v1/sales")

    @log_request_response
    async def get_project_by_project_id(self, project_id: int):
        return await self._make_request("GET", f"v1/projects/{project_id}")

    @log_request_response
    async def get_logins_by_project_id(
        self,
        project_id: int,
        limit: int = 50,
        offset: int = 0,
        status: Optional[str] = None,
        campus_id: Optional[str] = None,
    ):
        params = {"limit": limit, "offset": offset}
        if status:
            params["status"] = status
        if campus_id:
            params["campusId"] = campus_id
        return await self._make_request(
            "GET", f"v1/projects/{project_id}/participants", params=params
        )

    @log_request_response
    async def get_participant_by_login(self, login: str):
        return await self._make_request("GET", f"v1/participants/{login}")

    @log_request_response
    async def get_participant_workstation_by_login(self, login: str):
        return await self._make_request("GET", f"v1/participants/{login}/workstation")

    @log_request_response
    async def get_soft_skill_by_login(self, login: str):
        return await self._make_request("GET", f"v1/participants/{login}/skills")

    @log_request_response
    async def get_participant_projects_by_login(
        self,
        login: str,
        limit: int = 10,
        offset: int = 0,
        status: Optional[str] = None,
    ):
        params = {"limit": limit, "offset": offset}
        if status:
            params["status"] = status
        return await self._make_request(
            "GET", f"v1/participants/{login}/projects", params=params
        )

    @log_request_response
    async def get_participant_project_by_login_and_project_id(
        self, login: str, project_id: int
    ):
        return await self._make_request(
            "GET", f"v1/participants/{login}/projects/{project_id}"
        )

    @batch_async_requests(100)
    @log_request_response
    async def get_points_by_login(self, login: str):
        return await self._make_request("GET", f"v1/participants/{login}/points")

    @log_request_response
    async def get_log_weekly_avg_hours_by_login_and_date(
        self, login: str, date: Optional[str] = None
    ):
        params = {"date": date} if date else {}
        return await self._make_request(
            "GET", f"v1/participants/{login}/logtime", params=params
        )

    @log_request_response
    async def get_participant_feedback_by_login(self, login: str):
        return await self._make_request("GET", f"v1/participants/{login}/feedback")

    @log_request_response
    async def get_xp_history_by_login(
        self, login: str, limit: int = 50, offset: int = 0
    ):
        params = {"limit": limit, "offset": offset}
        return await self._make_request(
            "GET", f"v1/participants/{login}/experience-history", params=params
        )

    @log_request_response
    async def get_participant_courses_by_login(
        self,
        login: str,
        limit: int = 10,
        offset: int = 0,
        status: Optional[str] = None,
    ):
        params = {"limit": limit, "offset": offset}
        if status:
            params["status"] = status
        return await self._make_request(
            "GET", f"v1/participants/{login}/courses", params=params
        )

    @log_request_response
    async def get_participant_course_by_login_and_course_id(
        self, login: str, course_id: int
    ):
        return await self._make_request(
            "GET", f"v1/participants/{login}/courses/{course_id}"
        )

    @log_request_response
    async def get_coalition_by_login(self, login: str):
        return await self._make_request("GET", f"v1/participants/{login}/coalition")

    @log_request_response
    async def get_badges_by_login(self, login: str):
        return await self._make_request("GET", f"v1/participants/{login}/badges")

    @log_request_response
    async def get_graph(self):
        return await self._make_request("GET", "v1/graph")

    @log_request_response
    async def get_events(
        self,
        from_date: str,
        to_date: str,
        type: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ):
        params = {"from": from_date, "to": to_date, "limit": limit, "offset": offset}
        if type:
            params["type"] = type
        return await self._make_request("GET", "v1/events", params=params)

    @log_request_response
    async def get_course_by_course_id(self, course_id: int):
        return await self._make_request("GET", f"v1/courses/{course_id}")

    @log_request_response
    async def get_participants_by_coalition_id(
        self, coalition_id: int, limit: int = 50, offset: int = 0
    ):
        params = {"limit": limit, "offset": offset}
        return await self._make_request(
            "GET", f"v1/coalitions/{coalition_id}/participants", params=params
        )

    @log_request_response
    async def get_participants_by_coalition_id_1(
        self,
        cluster_id: int,
        limit: int = 50,
        offset: int = 0,
        occupied: Optional[bool] = None,
    ):
        params = {"limit": limit, "offset": offset}
        if occupied is not None:
            params["occupied"] = occupied
        return await self._make_request(
            "GET", f"v1/clusters/{cluster_id}/map", params=params
        )

    @paginated_request()
    @log_request_response
    async def get_campuses(self):
        return await self._make_request("GET", "v1/campuses")

    @paginated_request()
    @log_request_response
    async def get_participants_by_campus_id(
        self, campus_id: str, limit: int = 50, offset: int = 0
    ):
        params = {"limit": limit, "offset": offset}
        return await self._make_request(
            "GET", f"v1/campuses/{campus_id}/participants", params=params
        )

    @log_request_response
    @batch_async_requests()
    @paginated_request()
    async def get_coalitions_by_campus(
        self, campus_id: str, limit: int = 50, offset: int = 0
    ):
        params = {"limit": limit, "offset": offset}
        return await self._make_request(
            "GET", f"v1/campuses/{campus_id}/coalitions", params=params
        )

    @log_request_response
    @batch_async_requests()
    async def get_clusters_by_campus(self, campus_id: str):
        return await self._make_request("GET", f"v1/campuses/{campus_id}/clusters")

    async def close(self):
        """Закрывает сессию."""
        if self.session and not self.session.closed:
            await self.session.close()

async def main():
    api = School21API()
    try:
        # print(api.headers)
        # result = await api.get_participants_by_campus_id(campus_id='6bfe3c56-0211-4fe1-9e59-51616caac4dd', limit=1000)
        # print(len(result['participants']))
        # with open('xuy.json', 'r') as f:
        #     participants = json.load(f)['participants']

        campuses = await api.get_campuses()
        result = await api.get_points_by_login(participants[:])

        with open('xuyishe.json','w') as f:
            json.dump(result, f, indent=4)

    finally:
        await api.close()

if __name__ == "__main__":
    asyncio.run(main())