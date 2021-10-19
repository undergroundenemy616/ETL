import backoff
import psycopg2
from psycopg2.extras import RealDictCursor


class FilmWorkAdapter:
    def __init__(self, dsn: dict,
                 chunk_size: int):

        self.dsn = dsn
        self.chunk_size = chunk_size

    @backoff.on_exception(backoff.expo, psycopg2.OperationalError)
    def get_db_connection(self):
        dsn_string = ' '.join([f'{key}={value}' for key, value in self.dsn.items()])
        pg_conn = psycopg2.connect(dsn=dsn_string, cursor_factory=RealDictCursor)
        return pg_conn

    @staticmethod
    def get_sql_for_m2m_person(table_name: str) -> str:
        """
        Получаем имена и id всех персон конкретного типа (актер, директор, режиссер) для FilmWork.
        """
        with_name = table_name.split('_')[2]
        as_name = ''.join([word[0] for word in table_name.split('_')])
        SQL = f'''
        {with_name} as (
        SELECT m.id, 
               string_agg(CAST(a.id AS TEXT), ',') ids,
               string_agg(a.first_name || ' ' || a.last_name, ',') persons_names
        FROM movies_filmwork m
            LEFT JOIN {table_name} {as_name} on m.id = {as_name}.filmwork_id 
            LEFT JOIN movies_person a on {as_name}.person_id = a.id
        GROUP BY m.id
        )
        '''
        return SQL

    @staticmethod
    def get_sql_for_m2m_role(table_name: str) -> str:
        with_name = table_name.split('_')[2]
        as_name = ''.join([word[0] for word in table_name.split('_')])
        SQL = f'''
        {with_name} as (
        SELECT p.id,
               string_agg(CAST({as_name}.filmwork_id AS TEXT), ',') ids
        FROM movies_person p
            LEFT JOIN {table_name} {as_name} on {as_name}.person_id = p.id
        GROUP BY p.id
        )
        '''
        return SQL

    def get_updated_table_sql(self, updated_at: str, state_name: str) -> str:
        """
        Получаем обновленные объекты из привязанных таблиц (genre, person).
        """
        if state_name == 'genre':
            SQL = f'''
            SELECT *
            FROM movies_{state_name} 
            WHERE updated_at > '{updated_at}'
            ORDER BY updated_at
            LIMIT {self.chunk_size}
            '''
        else:
            with_actors_films_sql = self.get_sql_for_m2m_role('movies_filmwork_actors')
            with_writers_films_sql = self.get_sql_for_m2m_role('movies_filmwork_writers')
            with_directors_films_sql = self.get_sql_for_m2m_role('movies_filmwork_directors')

            SQL = f'''
        WITH roles as (
            SELECT p.id, 
                   string_agg(r.title, ',') roles
            FROM movies_person p
                LEFT JOIN movies_person_roles pr on p.id = pr.person_id 
                LEFT JOIN movies_role r on r.id = pr.role_id
            GROUP BY p.id
        ),
        {with_actors_films_sql},
        {with_writers_films_sql},
        {with_directors_films_sql}
        
            SELECT p.id,
                   p.updated_at,
                   p.first_name || ' ' || p.last_name full_name,
                   p.birth_day birth,
                   roles.roles roles,
                   actors.ids films_as_actor,
                   writers.ids films_as_writer,
                   directors.ids films_as_director
                
            FROM movies_{state_name} p
                LEFT JOIN roles ON roles.id = p.id
                LEFT JOIN actors ON actors.id = p.id
                LEFT JOIN writers ON writers.id = p.id
                LEFT JOIN directors ON directors.id = p.id
                
            WHERE updated_at > '{updated_at}'
            ORDER BY updated_at
            LIMIT {self.chunk_size}
            '''
        return SQL

    @staticmethod
    def get_updated_filmworks_ids_by_state_name(ids: str, state_name: str) -> str:
        """
        Получаем id FilmWork, которые будут обновлены за счет обновлений конкретной привязнной таблицы (genre, person).
        """
        if state_name == 'person':
            SQL = f'''
            SELECT fm.id,
                   fm.updated_at
            FROM movies_filmwork fm
                LEFT JOIN movies_filmwork_actors a ON a.filmwork_id = fm.id
                LEFT JOIN movies_filmwork_writers w ON w.filmwork_id = fm.id
                LEFT JOIN movies_filmwork_directors d ON d.filmwork_id = fm.id
            WHERE a.person_id IN ({ids}) OR 
                  w.person_id IN ({ids}) OR 
                  d.person_id in ({ids})
            ORDER BY fm.updated_at
            '''
        else:
            SQL = f'''
            SELECT fm.id,
                   fm.updated_at
            FROM movies_filmwork fm
                LEFT JOIN movies_filmwork_genres g ON g.filmwork_id = fm.id
            WHERE g.genre_id IN ({ids})
            ORDER BY fm.updated_at
            '''
        return SQL

    def get_updated_filmworks_by_ids_sql(self, filmworks_ids: str) -> str:
        """
        Получаем обновленные FilmWork по спсику их id.
        """
        SQL = f'''
        {self.__get_base_filmwork_sql()}
        WHERE fm.id in ({filmworks_ids})
        '''
        return SQL

    def get_updated_filmworks_sql(self, updated_at: str) -> str:
        """
        Получаем обновленные FilmWork по полю updated_at.
        """
        SQL = f'''
        {self.__get_base_filmwork_sql()}
        WHERE fm.updated_at > '{updated_at}'
        ORDER BY fm.updated_at
        LIMIT {self.chunk_size}
        '''
        return SQL

    def __get_base_filmwork_sql(self) -> str:
        """
        Базовый запрос для получения объектов FilmWork.
        """
        with_actors_sql = self.get_sql_for_m2m_person('movies_filmwork_actors')
        with_writers_sql = self.get_sql_for_m2m_person('movies_filmwork_writers')
        with_directors_sql = self.get_sql_for_m2m_person('movies_filmwork_directors')
        SQL = f'''
        WITH genres as (
            SELECT m.id, 
                   string_agg(CAST(g.id AS TEXT), ',') ids,
                   string_agg(g.title, ',') titles
            FROM movies_filmwork m
                LEFT JOIN movies_filmwork_genres mfg on m.id = mfg.filmwork_id 
                LEFT JOIN movies_genre g on mfg.genre_id = g.id
            GROUP BY m.id
        ),
        {with_actors_sql},
        {with_directors_sql},
        {with_writers_sql}

        SELECT 
               fm.updated_at,
               fm.id,
               fm.rating,
               fm.type,
               genres.ids genres_ids,
               genres.titles genres_titles,
               fm.title,
               fm.description,
               actors.ids actors_ids,
               actors.persons_names actors_names,
               writers.ids writers_ids,
               writers.persons_names writers_names,
               directors.ids directors_ids,
               directors.persons_names directors_names
        FROM movies_filmwork fm
            LEFT JOIN genres ON genres.id = fm.id
            LEFT JOIN actors ON actors.id = fm.id
            LEFT JOIN writers ON writers.id = fm.id
            LEFT JOIN directors ON directors.id = fm.id
        '''
        return SQL
