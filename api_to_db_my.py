# import sqlite3
import enum
import json
import uuid
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List, Callable
from functools import wraps

import traceback
import sqlalchemy
from sshtunnel import SSHTunnelForwarder

import s21_api
import pandas as pd
import asyncio
import psycopg2
from getpass import getpass
from sqlalchemy import JSON, create_engine
from sqlalchemy import String, Integer, DateTime, UUID, Boolean, Float, text
from sqlalchemy import Table, Column, MetaData, Enum as EnumType
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import ForeignKey
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy.orm import sessionmaker, relationship, declarative_base

from model_porject_info import create_from_json, ProjectDatabase

class ApiDataSaver:
    def __init__(self, db_path='school21.db'):
        self.db_path = db_path
        db_params = {
            'user_name' : 'postgres',
            'host' : 'localhost',
            'port' : 5432,
            'dbname' : self.db_path,
        }
        
        base_engine = create_engine(
            f"postgresql+psycopg2://{db_params['user_name']}@{db_params['host']}:{db_params['port']}/postgres",
        )
        with base_engine.connect().execution_options(isolation_level='AUTOCOMMIT') as conn:
            result = conn.execute(
                text("SELECT 1 FROM pg_catalog.pg_database WHERE datname = :dbname;"),
                {'dbname': db_params['dbname']}
            )
            if not result.fetchone():
                conn.execute(
                    text(f"CREATE DATABASE {db_params['dbname']};")
                )
        self.engine = create_engine(f"postgresql://{db_params['user_name']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")
        self.meta = MetaData()
        self.meta.reflect(self.engine)

        self._create_tables()

    @staticmethod
    def batch_async_requests(concurrency_limit: int = 10):
        """
        Декоратор для выполнения асинхронных запросов с поддержкой семафора.
        Принимает массив переменных, создаёт задачи для асинхронных запросов и объединяет результаты в один словарь.

        :param concurrency_limit: Максимальное количество одновременных запросов.
        """
        def decorator(func: Callable):
            @wraps(func)
            async def wrapper(self, items: List[Any], *args, **kwargs):
                # Семафор для ограничения количества одновременных запросов
                semaphore = asyncio.Semaphore(concurrency_limit)

                async def fetch_item(item: Any):
                    async with semaphore:  # Ограничиваем количество одновременных запросов
                        return await func(self, item, *args, **kwargs)

                tasks = [fetch_item(item) for item in items]
                results = await asyncio.gather(*tasks)

                return results

            return wrapper

        return decorator     

    def _upsert(self, df: pd.DataFrame, table: Table):
        with self.engine.connect() as connection:
            # Преобразуем DataFrame в список словарей
            data = df.to_dict(orient='records')

            # Получаем имена столбцов с первичным ключом
            primary_keys = [col.name for col in table.primary_key.columns]

            # Определяем, какие колонки имеют непустые значения
            non_empty_columns = [col for col in df.columns if not df[col].isna().all()]

            # Создаём INSERT-запрос
            stmt = insert(table).values(data)

            # Фильтруем колонки, чтобы не обновлять пустые значения
            update_dict = {
                col.name: stmt.excluded[col.name] 
                for col in table.columns 
                if col.name not in primary_keys and col.name in non_empty_columns
            }

            # Если есть что обновлять, добавляем `ON CONFLICT`
            if update_dict:
                stmt = stmt.on_conflict_do_update(
                    index_elements=primary_keys,
                    set_=update_dict
                )

            # Выполняем запрос
            connection.execute(stmt)
            connection.commit()

    def _create_tables(self):
        Base = declarative_base()
        
        class Campuses(Base):
            __tablename__ = 'campuses'

            id = Column(UUID(as_uuid=True), unique=True)
            shortName = Column(String, primary_key=True, nullable=False)
            fullName = Column(String, nullable=False)

            def __repr__(self):
                return f'<campuses(shortName={self.shortName}, id={self.id}, fullName={self.fullName})>'
        
        class Coalitions(Base):
            __tablename__ = 'coalitions'

            coalitionId = Column(Integer, primary_key=True, nullable=False)
            name = Column(String)
            campusId = Column(UUID(as_uuid=True), ForeignKey('campuses.id'))

            def __repr__(self):
                return f'<coalitions(name={self.name}, coalitionId={self.coalitionId}, campusId={self.campusId})>'

        statuses = ['ACTIVE', 'TEMPORARY_BLOCKING', 'EXPELLED', 'BLOCKED', 'FROZEN']
        Statuses = Enum('participantsStatuses', {status: i for i, status in enumerate(statuses)})

        class Participants(Base):
            __tablename__ = 'participants'

            login = Column(String, primary_key=True, nullable=False)
            className = Column(String)
            parallelName = Column(String)
            expValue = Column(Integer)
            level = Column(Integer)
            expToNextLevel = Column(Integer)
            campusShortName = Column(String, ForeignKey('campuses.shortName'))
            status = Column(EnumType(Statuses))
            coalitionId = Column(Integer, ForeignKey('coalitions.coalitionId'))
            studentId = Column(UUID(as_uuid=True), unique=True)
            userId = Column(UUID(as_uuid=True), unique=True)
            isActive = Column(Boolean)
            isGraduate = Column(Boolean)
            peerReviewPoints = Column(Integer)
            codeReviewPoints = Column(Integer)
            coins = Column(Integer)
            averageVerifierPunctuality = Column(Float)
            averageVerifierInterest = Column(Float)
            averageVerifierThoroughness = Column(Float)
            averageVerifierFriendliness = Column(Float)
            
        types = ['INDIVIDUAL', 'GROUP', 'EXAM', 'EXAM_TEST', 'INTERNSHIP']
        Types = Enum('projectsTypes', {type: i for i, type in enumerate(types)})

        
        class Projects(Base):
            __tablename__ = 'projects'

            id = Column(Integer, primary_key=True, nullable=False)
            title = Column(String, nullable=False)
            description = Column(String)
            type = Column(EnumType(Types))
            durationHours = Column(Integer)
            xp = Column(Integer)
            startCondition = Column(JSON)
            courseId = Column(Integer, ForeignKey('courses.courseId'))
            
        class Courses(Base):
            __tablename__ = 'courses'

            courseId = Column(Integer, primary_key=True, nullable=False)
            title = Column(String)
            description = Column(String)
            durationHours = Column(Integer)
            xp = Column(Integer)
            startCondition = Column(JSON)

            

        Base.metadata.create_all(bind=self.engine)

    def process_campuses(self, campuses):
        print(Table('campuses',self.meta,autoload_with=self.engine))
        try: 
            df = pd.DataFrame(
                pd.json_normalize(
                    campuses['campuses']
                    )
                )   
            self._upsert(df, Table('campuses', self.meta, autoload_with=self.engine))
        except Exception as e:
            if self.engine:
                self.engine.dispose()
            print(f'Error processing campuses: {e}')
    
    def process_coalitions(self, coalitions: dict):
        try:
            for campusId, coalitions in coalitions.items():
                df = pd.DataFrame(
                    pd.json_normalize(
                        coalitions['coalitions']
                        )
                    )
                df['campusId'] = campusId
                print(df)   
                self._upsert(df, Table('coalitions', self.meta, autoload_with=self.engine))
        except Exception as e:
            if self.engine:
                self.engine.dispose()
            print(f'Error processing coalitions: {e}')

    def process_participants_by_coalition(self, participants: dict):
        try:
            for coalitionId, participants in participants.items():
                if isinstance(participants, dict):    
                    df = pd.DataFrame({'login' : participants['participants']})
                    df['coalitionId'] = coalitionId
                    self._upsert(df, Table('participants', self.meta, autoload_with=self.engine))
                    
        except Exception as e:
            if self.engine:
                self.engine.dispose()
            print(f'Error processing participants: {e}')
            traceback.print_exc() 

    def process_participants_points(self, points: dict):
        try:
            df = pd.DataFrame(points).T.reset_index().rename(columns={'index' : 'login'})
            self._upsert(df, Table('participants', self.meta, autoload_with=self.engine))            
        except Exception as e:
            if self.engine:
                self.engine.dispose()
            print(f'Error processing points: {e}')
            traceback.print_exc()  

    async def process_participant_basic_info(self, logins: list[str]):
        # logins = pd.read_sql('''
        # SELECT p.login 
        # FROM participants AS p 
        # WHERE p.login NOT IN ('blossola', 'bloodywo') 
        # ORDER BY p.login ASC
        # OFFSET 20450
        # '''
        # , self.engine)
        # points = await self.api.get_points_by_login(logins['login'].values[10000:])
        # self.process_participants_points(points)

        # def split_list(lst, n):
        #     size = len(lst) // n
        #     remainder = len(lst) % n
        #     result = []
        #     start = 0
        #     for i in range(n):
        #         end = start + size + (1 if i < remainder else 0)
        #         result.append(lst[start:end])
        #         start = end
        #     return result

        
        # logins = split_list(list(logins['login'].values), 1)
        

        # for i, login in enumerate(logins):
        #     print(i) 
        #     basicInfo = await self.api.get_participant_by_login(login)
        #     df = pd.json_normalize(basicInfo.values())
        #     df.rename(columns={'campus.shortName':'campusShortName'}, inplace=True)
        #     df.drop('campus.id',inplace=True,axis=1)
        #     self._upsert(df, Table('participants', self.meta, autoload_with=self.engine))

        # for i, login in enumerate(logins):
        #     print(i) 
        #     basicInfo = await self.api.get_participant_feedback_by_login(login)           
        #     df = pd.DataFrame(basicInfo).T.reset_index().rename(columns={'index':'login'})

        #     self._upsert(df, Table('participants', self.meta, autoload_with=self.engine))
        
        for i, login in enumerate(logins):
            print(i) 
            basicInfo = await self.api.get_participant_credentials_by_login(login)
            df = pd.DataFrame(basicInfo).T.reset_index().rename(columns={'index':'login'})
            df.drop(columns=['schoolId'], inplace=True)
            self._upsert(df, Table('participants', self.meta, autoload_with=self.engine))

    def close(self):
        if self.engine:
            self.engine.dispose()
    
    def __del__(self):
        self.close()

async def main():
    api = s21_api.School21API()
    api_data_saver = ApiDataSaver('test2')
    try:
        pass
        # campuses = await api.get_campuses()
        # api_data_saver.process_campuses(campuses)

        # campuses = pd.read_sql('SELECT id FROM campuses', api_data_saver.engine)
        # coalitions = await api.get_coalitions_by_campus(campuses['id'].values[:])
        # api_data_saver.process_coalitions(coalitions)

        # coalitions = pd.read_sql('SELECT * FROM coalitions', api_data_saver.engine)
        # participants = await api.get_participants_by_coalition_id(coalitions['coalitionId'].values[:])
        # api_data_saver.process_participants_by_coalition(participants)

        # login = pd.read_sql('SELECT login FROM participants', api_data_saver.engine)['login'].values
        
        # basicInfo = await api.get_participant_credentials_by_login(login)
        # df = pd.DataFrame(basicInfo).T.reset_index().rename(columns={'index':'login'})
        # df.drop(columns=['schoolId'], inplace=True)
        # api_data_saver._upsert(df, Table('participants', api_data_saver.meta, autoload_with=api_data_saver.engine))
        
        # data = await api.get_participant_projects_by_login('batwomal')
        # with open('projects.json', 'r') as f:
        #     data = json.load(f)
        # data = pd.DataFrame(data['projects'])
        # data = pd.DataFrame(data['courseId'].drop_duplicates().fillna(0).astype('int'), columns=['courseId'])
        # data.sort_values(by=['courseId'], inplace=True)
        # data.reset_index(drop=True, inplace=True)
        # data.drop(0, inplace=True)

        
        
        # api_data_saver._upsert(
        #     data,
        #     Table('courses', api_data_saver.meta, autoload_with=api_data_saver.engine)
        # )
        # data = []
        # data.append( await api.get_project_by_project_id(21389))
        # print(pd.DataFrame(data))

        # projects = pd.read_sql('SELECT id FROM projects', api_data_saver.engine)
        # projects = [int(i) for i in projects['id'].values]
        ProjectDatabase.cleanup(api_data_saver.engine)
        with open ('projects.json') as f:
            projects = json.load(f)

        projects = [project['id'] for project in projects['projects']]
        # print(projects)
        data = await api.getProjectInfo(projects[:12])

        for i , project in enumerate(data.values()):
            print(i)    
            create_from_json(api_data_saver.engine, project['data'])

    except Exception as e:
        print(e)
    finally:
        api_data_saver.close()
        await api.close()


if __name__ == '__main__':
    asyncio.run(main())
