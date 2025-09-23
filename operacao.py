import streamlit as st
import pandas as pd
import sqlite3
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import numpy as np
import base64
import os
import logging
import threading
import time
from contextlib import contextmanager
from dotenv import load_dotenv

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_base64_image(image_path):
    """Converte imagem local para base64 para exibição no Streamlit"""
    try:
        with open(image_path, "rb") as img_file:
            return base64.b64encode(img_file.read()).decode()
    except FileNotFoundError:
        return "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="


# Configuração da página
st.set_page_config(
    page_title="BI Rezende Energia",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="⚡"
)

# CSS Personalizado
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    .main {
        font-family: 'Inter', sans-serif;
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
    }

    .custom-header {
        background: linear-gradient(135deg, #F7931E 0%, #e67e22 100%);
        padding: 2rem;
        border-radius: 15px;
        margin-bottom: 2rem;
        box-shadow: 0 8px 32px rgba(247, 147, 30, 0.3);
        color: white;
        text-align: center;
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 2rem;
    }

    .custom-header .logo-container {
        flex-shrink: 0;
    }

    .custom-header .logo-container img {
        height: 80px;
        width: auto;
        filter: brightness(0) invert(1);
    }

    .custom-header .text-container {
        flex-grow: 1;
    }

    .custom-header h1 {
        margin: 0;
        font-size: 2.5rem;
        font-weight: 700;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
    }

    .custom-header p {
        margin: 0.5rem 0 0 0;
        font-size: 1.1rem;
        opacity: 0.9;
    }

    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.08);
        border-left: 5px solid #F7931E;
        margin-bottom: 1rem;
        transition: transform 0.3s ease, box-shadow 0.3s ease;
    }

    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 25px rgba(0,0,0,0.15);
    }

    .cache-status {
        background: #e8f5e8;
        border: 1px solid #4caf50;
        border-radius: 8px;
        padding: 0.5rem 1rem;
        margin: 1rem 0;
        font-size: 0.85rem;
        color: #2e7d32;
    }

    .cache-status.updating {
        background: #fff3e0;
        border-color: #ff9800;
        color: #f57c00;
    }

    .cache-status.error {
        background: #ffebee;
        border-color: #f44336;
        color: #c62828;
    }

    .update-progress {
        background: #e3f2fd;
        border: 1px solid #2196f3;
        border-radius: 8px;
        padding: 1rem;
        margin: 1rem 0;
        font-size: 0.9rem;
        color: #1976d2;
    }

    .stButton > button {
        background: linear-gradient(135deg, #F7931E 0%, #e67e22 100%);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.5rem 1.5rem;
        font-weight: 600;
        transition: all 0.3s ease;
        box-shadow: 0 2px 10px rgba(247, 147, 30, 0.3);
    }

    .stButton > button:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 15px rgba(247, 147, 30, 0.4);
    }

    .stButton > button:disabled {
        background: #cccccc;
        color: #666666;
        cursor: not-allowed;
        transform: none;
        box-shadow: none;
    }

    [data-testid="metric-container"] {
        background: white;
        border: 1px solid #e9ecef;
        padding: 1rem;
        border-radius: 12px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.05);
        border-left: 5px solid #F7931E;
    }

    [data-testid="metric-container"] > div {
        color: #000000;
    }

    .chart-container {
        background: white;
        border-radius: 12px;
        padding: 1.5rem;
        box-shadow: 0 4px 20px rgba(0,0,0,0.08);
        margin-bottom: 2rem;
    }

    .chart-title {
        font-size: 1.2rem;
        font-weight: 600;
        color: #2c3e50;
        margin-bottom: 1rem;
        text-align: center;
    }

    @media (max-width: 768px) {
        .custom-header {
            flex-direction: column;
            gap: 1rem;
        }
        .custom-header h1 {
            font-size: 2rem;
        }
        .custom-header .logo-container img {
            height: 60px;
        }
    }
</style>
""", unsafe_allow_html=True)


class DataManager:
    """Gerenciador de dados com modelo dimensional e cache SQLite"""

    def __init__(self):
        self.db_path = "rezende_cache.db"
        self.cache_duration_hours = 1
        self.daily_cache_duration = 24
        self.is_cloud = os.environ.get('STREAMLIT_CLOUD', False)
        self.update_lock = threading.Lock()

        # Equipes excluídas do faturamento
        self.equipes_excluidas = [
            'RZ_JUPITER_01',
            'RZ_JUPITER_02',
            'RZ_CONCRETO_01',
            'EQ_TREINAMENTO_NW'
        ]

        try:
            self.redshift_config = {
                "host": st.secrets["redshift"]["host"],
                "port": st.secrets["redshift"]["port"],
                "database": st.secrets["redshift"]["database"],
                "user": st.secrets["redshift"]["user"],
                "password": st.secrets["redshift"]["password"]
            }
        except Exception as e:
            logger.error(f"Erro ao carregar configurações do Redshift: {e}")
            raise

        try:
            self.init_cache_db()
        except Exception as e:
            logger.error(f"Erro ao inicializar cache: {e}")

    def get_equipes_excluidas_sql(self):
        """Retorna cláusula SQL para excluir equipes específicas"""
        equipes_str = "', '".join(self.equipes_excluidas)
        return f"AND s.des_equipe_rcb NOT IN ('{equipes_str}')"

    def health_check(self):
        """Verifica se o sistema está funcionando corretamente"""
        checks = {
            'sqlite_connection': False,
            'redshift_connection': False,
            'cache_initialized': False,
            'tables_exist': False
        }

        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                conn.execute("SELECT 1")
                checks['sqlite_connection'] = True

                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*) FROM sqlite_master 
                    WHERE type='table' AND name IN ('cache_metadata', 'dim_calendario', 'dim_equipes', 'fato_servicos')
                """)
                table_count = cursor.fetchone()[0]
                checks['tables_exist'] = table_count >= 4
                checks['cache_initialized'] = table_count > 0

        except Exception as e:
            logger.error(f"Erro no health check SQLite: {e}")

        try:
            with self.get_redshift_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    checks['redshift_connection'] = True
        except Exception as e:
            logger.error(f"Erro no health check Redshift: {e}")

        return checks

    def get_equipes_excluidas_sqlite(self):
        """Retorna cláusula SQLite para excluir equipes específicas"""
        placeholders = ','.join(['?' for _ in self.equipes_excluidas])
        return f"AND nome_equipe NOT IN ({placeholders})", self.equipes_excluidas

    def init_cache_db(self):
        """Inicializa o banco SQLite de cache com modelo dimensional"""
        try:
            with sqlite3.connect(self.db_path, timeout=30) as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA temp_store=MEMORY")
                conn.execute("PRAGMA mmap_size=268435456")

                # Tabela de metadados do cache
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS cache_metadata (
                        table_name TEXT PRIMARY KEY,
                        last_update TIMESTAMP,
                        record_count INTEGER,
                        status TEXT DEFAULT 'active',
                        update_duration_seconds INTEGER DEFAULT 0
                    )
                """)

                # DIMENSÃO CALENDÁRIO
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS dim_calendario (
                        data_key DATE PRIMARY KEY,
                        ano INTEGER,
                        mes INTEGER,
                        dia INTEGER,
                        dia_semana INTEGER,
                        nome_dia_semana TEXT,
                        nome_mes TEXT,
                        trimestre INTEGER,
                        semana_ano INTEGER,
                        is_fim_semana BOOLEAN,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                # DIMENSÃO EQUIPES
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS dim_equipes (
                        cod_equipe TEXT PRIMARY KEY,
                        nome_equipe TEXT,
                        regional TEXT,
                        is_ativa BOOLEAN DEFAULT TRUE,
                        is_excluida BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                # FATO SERVIÇOS
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS fato_servicos (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        data_execucao DATE,
                        cod_equipe TEXT,
                        nome_equipe TEXT,
                        descricao_servico TEXT,
                        quantidade INTEGER,
                        valor_total REAL,
                        valor_unitario REAL,
                        regional TEXT,
                        nota TEXT,
                        placa TEXT,
                        cidade TEXT,
                        cod_obra TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (data_execucao) REFERENCES dim_calendario(data_key),
                        FOREIGN KEY (cod_equipe) REFERENCES dim_equipes(cod_equipe)
                    )
                """)

                # FATO METAS
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS fato_metas (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        data_meta DATE,
                        cod_equipe TEXT,
                        nome_equipe TEXT,
                        valor_meta REAL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (data_meta) REFERENCES dim_calendario(data_key),
                        FOREIGN KEY (cod_equipe) REFERENCES dim_equipes(cod_equipe)
                    )
                """)

                # FATO FATURAMENTO DIÁRIO
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS fato_faturamento_diario (
                        data DATE PRIMARY KEY,
                        faturamento_diario REAL,
                        servicos_dia INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (data) REFERENCES dim_calendario(data_key)
                    )
                """)

                # Índices para performance
                conn.execute("CREATE INDEX IF NOT EXISTS idx_fato_servicos_data ON fato_servicos(data_execucao)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_fato_servicos_equipe ON fato_servicos(cod_equipe)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_fato_servicos_nome_equipe ON fato_servicos(nome_equipe)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_fato_metas_data ON fato_metas(data_meta)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_fato_metas_equipe ON fato_metas(cod_equipe)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_dim_calendario_ano_mes ON dim_calendario(ano, mes)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_dim_equipes_regional ON dim_equipes(regional)")

                conn.commit()
                logger.info("Cache SQLite inicializado com sucesso")

        except Exception as e:
            logger.error(f"Erro ao inicializar cache: {e}")
            raise

    @contextmanager
    def get_redshift_connection(self):
        """Context manager para conexões com Redshift com timeout"""
        conn = None
        try:
            # Adicionar timeout para evitar travamentos
            conn = psycopg2.connect(
                connect_timeout=30,
                **self.redshift_config
            )
            conn.autocommit = True
            yield conn
        except Exception as e:
            logger.error(f"Erro na conexão Redshift: {e}")
            raise
        finally:
            if conn:
                try:
                    conn.close()
                except:
                    pass

    def needs_refresh(self, table_name, custom_hours=None):
        """Verifica se o cache precisa ser atualizado"""
        hours = custom_hours or self.cache_duration_hours

        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name='cache_metadata'
                """)

                if not cursor.fetchone():
                    return True, "Cache não inicializado"

                cursor.execute("""
                    SELECT last_update, record_count, status FROM cache_metadata 
                    WHERE table_name = ?
                """, (table_name,))

                result = cursor.fetchone()
                if not result or not result[0]:
                    return True, "Cache não existe"

                # Verificar se está em processo de atualização
                if result[2] == 'updating':
                    return False, "Atualizando..."

                last_update = datetime.fromisoformat(result[0])
                time_diff = datetime.now() - last_update

                if time_diff > timedelta(hours=hours):
                    return True, f"Cache expirado ({time_diff})"

                return False, f"Cache válido (atualizado há {time_diff})"

        except Exception as e:
            logger.error(f"Erro ao verificar cache {table_name}: {e}")
            return True, f"Erro na verificação: {e}"

    def update_cache_metadata(self, table_name, record_count, duration_seconds=0, status='active'):
        """Atualiza metadados do cache"""
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO cache_metadata 
                    (table_name, last_update, record_count, status, update_duration_seconds)
                    VALUES (?, ?, ?, ?, ?)
                """, (table_name, datetime.now().isoformat(), record_count, status, duration_seconds))
                conn.commit()
        except Exception as e:
            logger.error(f"Erro ao atualizar metadados do cache: {e}")

    def set_updating_status(self, table_name, is_updating=True):
        """Marca tabela como em processo de atualização"""
        try:
            status = 'updating' if is_updating else 'active'
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO cache_metadata 
                    (table_name, last_update, record_count, status)
                    VALUES (?, ?, 0, ?)
                """, (table_name, datetime.now().isoformat(), status))
                conn.commit()
        except Exception as e:
            logger.error(f"Erro ao definir status de atualização: {e}")

    def refresh_dimensao_calendario(self, force=False):
        """Atualiza dimensão calendário"""
        with self.update_lock:
            needs_update, reason = self.needs_refresh('dim_calendario', 168)

            if not needs_update and not force:
                return False, reason

            try:
                start_time = time.time()
                self.set_updating_status('dim_calendario', True)
                logger.info("Iniciando atualização da dimensão calendário...")

                start_date = datetime.now() - timedelta(days=730)
                end_date = datetime.now() + timedelta(days=365)

                calendar_data = []
                current_date = start_date

                while current_date <= end_date:
                    calendar_data.append({
                        'data_key': current_date.strftime('%Y-%m-%d'),
                        'ano': current_date.year,
                        'mes': current_date.month,
                        'dia': current_date.day,
                        'dia_semana': current_date.weekday() + 1,
                        'nome_dia_semana': current_date.strftime('%A'),
                        'nome_mes': current_date.strftime('%B'),
                        'trimestre': (current_date.month - 1) // 3 + 1,
                        'semana_ano': current_date.isocalendar()[1],
                        'is_fim_semana': current_date.weekday() >= 5
                    })
                    current_date += timedelta(days=1)

                df_calendario = pd.DataFrame(calendar_data)

                with sqlite3.connect(self.db_path, timeout=30) as conn:
                    conn.execute("DELETE FROM dim_calendario")
                    df_calendario.to_sql('dim_calendario', conn, if_exists='append', index=False)
                    conn.commit()

                duration = int(time.time() - start_time)
                self.update_cache_metadata('dim_calendario', len(df_calendario), duration)
                logger.info(f"Dimensão calendário atualizada: {len(df_calendario)} registros em {duration}s")

                return True, f"Dimensão calendário atualizada com {len(df_calendario)} registros"

            except Exception as e:
                logger.error(f"Erro ao atualizar dimensão calendário: {e}")
                self.update_cache_metadata('dim_calendario', 0, 0, 'error')
                return False, f"Erro: {e}"

    def refresh_dimensao_equipes(self, force=False):
        """Atualiza dimensão equipes"""
        with self.update_lock:
            needs_update, reason = self.needs_refresh('dim_equipes')

            if not needs_update and not force:
                return False, reason

            try:
                start_time = time.time()
                self.set_updating_status('dim_equipes', True)
                logger.info("Iniciando atualização da dimensão equipes...")

                query = """
                SELECT DISTINCT 
                    s.cod_equipe_rcb as cod_equipe,
                    s.des_equipe_rcb as nome_equipe,
                    s.num_cont_rcb as regional
                FROM res_cubo_servico s
                WHERE s.cod_equipe_rcb IS NOT NULL 
                    AND s.des_equipe_rcb IS NOT NULL
                UNION
                SELECT DISTINCT 
                    m.cod_equipe_cms as cod_equipe,
                    s.des_equipe_rcb as nome_equipe,
                    s.num_cont_rcb as regional
                FROM res_cubo_servico_meta m
                LEFT JOIN res_cubo_servico s ON m.cod_equipe_cms = s.cod_equipe_rcb
                WHERE m.cod_equipe_cms IS NOT NULL
                ORDER BY cod_equipe
                """

                with self.get_redshift_connection() as conn:
                    df_equipes = pd.read_sql_query(query, conn)

                if df_equipes.empty:
                    self.update_cache_metadata('dim_equipes', 0, 0, 'error')
                    return False, "Nenhuma equipe encontrada no Redshift"

                df_equipes['is_excluida'] = df_equipes['nome_equipe'].isin(self.equipes_excluidas)
                df_equipes['is_ativa'] = ~df_equipes['is_excluida']

                with sqlite3.connect(self.db_path, timeout=30) as conn:
                    conn.execute("DELETE FROM dim_equipes")
                    df_equipes.to_sql('dim_equipes', conn, if_exists='append', index=False)
                    conn.commit()

                duration = int(time.time() - start_time)
                self.update_cache_metadata('dim_equipes', len(df_equipes), duration)
                logger.info(f"Dimensão equipes atualizada: {len(df_equipes)} registros em {duration}s")

                return True, f"Dimensão equipes atualizada com {len(df_equipes)} registros"

            except Exception as e:
                logger.error(f"Erro ao atualizar dimensão equipes: {e}")
                self.update_cache_metadata('dim_equipes', 0, 0, 'error')
                return False, f"Erro: {e}"

    def refresh_fato_servicos(self, force=False):
        """Atualiza fato serviços com timeout e controle de bloqueio"""
        with self.update_lock:
            needs_update, reason = self.needs_refresh('fato_servicos')

            if not needs_update and not force:
                return False, reason

            try:
                start_time = time.time()
                self.set_updating_status('fato_servicos', True)
                logger.info("Iniciando atualização do fato serviços...")

                query = f"""
                SELECT 
                    s.dta_exec_rcb as data_execucao,
                    s.cod_equipe_rcb as cod_equipe,
                    s.des_equipe_rcb as nome_equipe,
                    s.des_preco_pre_rcb as descricao_servico,
                    COUNT(*) as quantidade,
                    COALESCE(SUM(s.vlr_total_serv_rcb), 0) as valor_total,
                    CASE 
                        WHEN COUNT(*) > 0 THEN COALESCE(SUM(s.vlr_total_serv_rcb), 0) / COUNT(*)
                        ELSE 0 
                    END as valor_unitario,
                    s.num_cont_rcb as regional,
                    COALESCE(o.cod_pep, 'N/A') as nota,
                    s.cod_placa_rcb as placa,
                    COALESCE(s.des_cida_rcb, 'N/A') as cidade,
                    s.cod_obra_rcb as cod_obra
                FROM res_cubo_servico s
                LEFT JOIN res_cubo_obra o ON s.cod_obra_rcb = o.cod_obra
                WHERE s.dta_exec_rcb IS NOT NULL 
                    AND s.dta_exec_rcb >= CURRENT_DATE - INTERVAL '90 days'
                    {self.get_equipes_excluidas_sql()}
                GROUP BY 
                    s.dta_exec_rcb, s.cod_equipe_rcb, s.des_equipe_rcb, s.des_preco_pre_rcb, s.num_cont_rcb, 
                    o.cod_pep, s.cod_placa_rcb, s.des_cida_rcb, s.cod_obra_rcb
                ORDER BY s.dta_exec_rcb DESC
                """

                with self.get_redshift_connection() as conn:
                    df_servicos = pd.read_sql_query(query, conn)

                if df_servicos.empty:
                    self.update_cache_metadata('fato_servicos', 0, 0, 'error')
                    return False, "Nenhum dado retornado do Redshift"

                with sqlite3.connect(self.db_path, timeout=30) as conn:
                    conn.execute("DELETE FROM fato_servicos")
                    df_servicos.to_sql('fato_servicos', conn, if_exists='append', index=False)
                    conn.commit()

                duration = int(time.time() - start_time)
                self.update_cache_metadata('fato_servicos', len(df_servicos), duration)
                logger.info(f"Fato serviços atualizado: {len(df_servicos)} registros em {duration}s")

                return True, f"Fato serviços atualizado com {len(df_servicos)} registros"

            except Exception as e:
                logger.error(f"Erro ao atualizar fato serviços: {e}")
                self.update_cache_metadata('fato_servicos', 0, 0, 'error')
                return False, f"Erro: {e}"

    def refresh_fato_metas(self, force=False):
        """Atualiza fato metas"""
        with self.update_lock:
            needs_update, reason = self.needs_refresh('fato_metas')

            if not needs_update and not force:
                return False, reason

            try:
                start_time = time.time()
                self.set_updating_status('fato_metas', True)
                logger.info("Iniciando atualização do fato metas...")

                query = f"""
                SELECT 
                    m.dta_meta_msd as data_meta,
                    m.cod_equipe_cms as cod_equipe,
                    s.des_equipe_rcb as nome_equipe,
                    COALESCE(m.vlr_meta_msd, 0) as valor_meta
                FROM res_cubo_servico_meta m
                LEFT JOIN res_cubo_servico s ON m.cod_equipe_cms = s.cod_equipe_rcb
                WHERE m.dta_meta_msd IS NOT NULL 
                    AND m.dta_meta_msd >= CURRENT_DATE - INTERVAL '90 days'
                    AND s.des_equipe_rcb IS NOT NULL
                    AND s.des_equipe_rcb NOT IN ('{"', '".join(self.equipes_excluidas)}')
                GROUP BY m.dta_meta_msd, m.cod_equipe_cms, s.des_equipe_rcb, m.vlr_meta_msd
                ORDER BY m.dta_meta_msd DESC
                """

                with self.get_redshift_connection() as conn:
                    df_metas = pd.read_sql_query(query, conn)

                if not df_metas.empty:
                    with sqlite3.connect(self.db_path, timeout=30) as conn:
                        conn.execute("DELETE FROM fato_metas")
                        df_metas.to_sql('fato_metas', conn, if_exists='append', index=False)
                        conn.commit()

                    duration = int(time.time() - start_time)
                    self.update_cache_metadata('fato_metas', len(df_metas), duration)
                    logger.info(f"Fato metas atualizado: {len(df_metas)} registros em {duration}s")
                    return True, f"Fato metas atualizado com {len(df_metas)} registros"

                self.update_cache_metadata('fato_metas', 0, 0, 'error')
                return False, "Nenhum dado de meta encontrado"

            except Exception as e:
                logger.error(f"Erro ao atualizar fato metas: {e}")
                self.update_cache_metadata('fato_metas', 0, 0, 'error')
                return False, f"Erro: {e}"

    def refresh_fato_faturamento_diario(self, force=False):
        """Atualiza fato faturamento diário"""
        with self.update_lock:
            needs_update, reason = self.needs_refresh('fato_faturamento_diario')

            if not needs_update and not force:
                return False, reason

            try:
                start_time = time.time()
                self.set_updating_status('fato_faturamento_diario', True)
                logger.info("Iniciando atualização do fato faturamento diário...")

                query = """
                SELECT 
                    data_execucao as data,
                    SUM(valor_total) as faturamento_diario,
                    SUM(quantidade) as servicos_dia
                FROM fato_servicos
                GROUP BY data_execucao
                ORDER BY data_execucao
                """

                with sqlite3.connect(self.db_path, timeout=30) as conn:
                    df_faturamento = pd.read_sql_query(query, conn)

                if not df_faturamento.empty:
                    with sqlite3.connect(self.db_path, timeout=30) as conn:
                        conn.execute("DELETE FROM fato_faturamento_diario")
                        df_faturamento.to_sql('fato_faturamento_diario', conn, if_exists='append', index=False)
                        conn.commit()

                    duration = int(time.time() - start_time)
                    self.update_cache_metadata('fato_faturamento_diario', len(df_faturamento), duration)
                    logger.info(f"Fato faturamento diário atualizado: {len(df_faturamento)} registros em {duration}s")
                    return True, f"Fato faturamento diário atualizado com {len(df_faturamento)} registros"

                self.update_cache_metadata('fato_faturamento_diario', 0, 0, 'error')
                return False, "Nenhum dado de faturamento encontrado"

            except Exception as e:
                logger.error(f"Erro ao atualizar fato faturamento diário: {e}")
                self.update_cache_metadata('fato_faturamento_diario', 0, 0, 'error')
                return False, f"Erro: {e}"

    def refresh_all_caches(self, force=False, progress_callback=None):
        """Atualiza todos os caches com controle de progresso e sem loops infinitos"""
        results = {}
        total_steps = 5
        current_step = 0

        # Prevenir múltiplas execuções simultâneas
        if hasattr(self, '_refreshing_all') and self._refreshing_all:
            return {'error': 'Atualização já em andamento'}

        try:
            self._refreshing_all = True

            # 1. Dimensão calendário
            current_step += 1
            if progress_callback:
                progress_callback(current_step, total_steps, "Atualizando dimensão calendário...")

            success, message = self.refresh_dimensao_calendario(force)
            results['dim_calendario'] = {'success': success, 'message': message}

            # 2. Dimensão equipes
            current_step += 1
            if progress_callback:
                progress_callback(current_step, total_steps, "Atualizando dimensão equipes...")

            success, message = self.refresh_dimensao_equipes(force)
            results['dim_equipes'] = {'success': success, 'message': message}

            # 3. Fato serviços
            current_step += 1
            if progress_callback:
                progress_callback(current_step, total_steps, "Atualizando fato serviços...")

            success, message = self.refresh_fato_servicos(force)
            results['fato_servicos'] = {'success': success, 'message': message}

            # 4. Fato metas
            current_step += 1
            if progress_callback:
                progress_callback(current_step, total_steps, "Atualizando fato metas...")

            success, message = self.refresh_fato_metas(force)
            results['fato_metas'] = {'success': success, 'message': message}

            # 5. Fato faturamento diário
            current_step += 1
            if progress_callback:
                progress_callback(current_step, total_steps, "Atualizando fato faturamento diário...")

            success, message = self.refresh_fato_faturamento_diario(force)
            results['fato_faturamento_diario'] = {'success': success, 'message': message}

            return results

        finally:
            self._refreshing_all = False

    def get_dimension_filters(self):
        """Busca opções de filtro das dimensões com tratamento de erro"""
        options = {
            'equipes': [],
            'cod_equipes': [],
            'regionais': [],
            'anos': [],
            'meses': [],
            'tipos_servico': [],
            'notas': [],
            'placas': []
        }

        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                # EQUIPES
                try:
                    df = pd.read_sql_query("""
                        SELECT DISTINCT 
                            fs.cod_equipe, 
                            fs.nome_equipe, 
                            fs.regional 
                        FROM fato_servicos fs
                        WHERE fs.nome_equipe IS NOT NULL
                        ORDER BY fs.nome_equipe
                    """, conn)
                    options['equipes'] = df['nome_equipe'].tolist()
                    options['cod_equipes'] = df['cod_equipe'].tolist()
                except:
                    pass

                # REGIONAIS
                try:
                    df_regionais = pd.read_sql_query("""
                        SELECT DISTINCT regional 
                        FROM fato_servicos 
                        WHERE regional IS NOT NULL
                        ORDER BY regional
                    """, conn)
                    options['regionais'] = df_regionais['regional'].tolist()
                except:
                    pass

                # ANOS
                try:
                    df = pd.read_sql_query("""
                        SELECT DISTINCT ano 
                        FROM dim_calendario 
                        ORDER BY ano DESC
                    """, conn)
                    options['anos'] = df['ano'].tolist()
                except:
                    pass

                # MESES
                try:
                    df = pd.read_sql_query("""
                        SELECT DISTINCT mes, nome_mes 
                        FROM dim_calendario 
                        ORDER BY mes
                    """, conn)
                    options['meses'] = df['nome_mes'].tolist()
                except:
                    pass

                # TIPOS DE SERVIÇO
                try:
                    df = pd.read_sql_query("""
                        SELECT DISTINCT descricao_servico 
                        FROM fato_servicos 
                        WHERE descricao_servico IS NOT NULL 
                        ORDER BY descricao_servico
                    """, conn)
                    options['tipos_servico'] = df['descricao_servico'].tolist()
                except:
                    pass

                # NOTAS
                try:
                    df = pd.read_sql_query("""
                        SELECT DISTINCT nota 
                        FROM fato_servicos 
                        WHERE nota IS NOT NULL AND nota != 'N/A'
                        ORDER BY nota
                    """, conn)
                    options['notas'] = df['nota'].tolist()
                except:
                    pass

                # PLACAS
                try:
                    df = pd.read_sql_query("""
                        SELECT DISTINCT placa 
                        FROM fato_servicos 
                        WHERE placa IS NOT NULL 
                        ORDER BY placa
                    """, conn)
                    options['placas'] = df['placa'].tolist()
                except:
                    pass

        except Exception as e:
            logger.error(f"Erro ao buscar opções de filtro: {e}")

        return options

    def get_servicos_data(self, filters=None):
        """Busca dados de serviços com JOINs dimensionais"""
        query = """
        SELECT 
            fs.*,
            de.regional as regional_equipe,
            dc.ano,
            dc.mes,
            dc.nome_mes,
            dc.nome_dia_semana
        FROM fato_servicos fs
        LEFT JOIN dim_equipes de ON fs.cod_equipe = de.cod_equipe
        LEFT JOIN dim_calendario dc ON fs.data_execucao = dc.data_key
        WHERE (de.is_excluida = FALSE OR de.is_excluida IS NULL)
        """
        params = []

        if filters:
            if filters.get('data_inicio'):
                query += " AND fs.data_execucao >= ?"
                params.append(filters['data_inicio'])

            if filters.get('data_fim'):
                query += " AND fs.data_execucao <= ?"
                params.append(filters['data_fim'])

            if filters.get('equipes'):
                placeholders = ','.join(['?' for _ in filters['equipes']])
                query += f" AND fs.nome_equipe IN ({placeholders})"
                params.extend(filters['equipes'])

            if filters.get('tipos_servico'):
                placeholders = ','.join(['?' for _ in filters['tipos_servico']])
                query += f" AND fs.descricao_servico IN ({placeholders})"
                params.extend(filters['tipos_servico'])

            if filters.get('regionais'):
                placeholders = ','.join(['?' for _ in filters['regionais']])
                query += f" AND fs.regional IN ({placeholders})"
                params.extend(filters['regionais'])

            if filters.get('notas'):
                placeholders = ','.join(['?' for _ in filters['notas']])
                query += f" AND fs.nota IN ({placeholders})"
                params.extend(filters['notas'])

            if filters.get('placas'):
                placeholders = ','.join(['?' for _ in filters['placas']])
                query += f" AND fs.placa IN ({placeholders})"
                params.extend(filters['placas'])

        query += " ORDER BY fs.data_execucao DESC"

        try:
            with sqlite3.connect(self.db_path, timeout=15) as conn:
                return pd.read_sql_query(query, conn, params=params)
        except Exception as e:
            logger.error(f"Erro ao buscar dados de serviços: {e}")
            return pd.DataFrame()

    def get_producao_vs_meta_data(self, filters=None):
        """Busca dados de produção vs meta com JOINs dimensionais"""

        # Query para produção
        query_producao = """
        SELECT 
            fs.nome_equipe as equipe,
            fs.cod_equipe,
            SUM(fs.valor_total) as producao_real,
            SUM(fs.quantidade) as quantidade_servicos
        FROM fato_servicos fs
        LEFT JOIN dim_equipes de ON fs.cod_equipe = de.cod_equipe
        LEFT JOIN dim_calendario dc ON fs.data_execucao = dc.data_key
        WHERE (de.is_excluida = FALSE OR de.is_excluida IS NULL)
        """
        params_producao = []

        if filters:
            if filters.get('data_inicio'):
                query_producao += " AND fs.data_execucao >= ?"
                params_producao.append(filters['data_inicio'])

            if filters.get('data_fim'):
                query_producao += " AND fs.data_execucao <= ?"
                params_producao.append(filters['data_fim'])

            if filters.get('equipes'):
                placeholders = ','.join(['?' for _ in filters['equipes']])
                query_producao += f" AND fs.nome_equipe IN ({placeholders})"
                params_producao.extend(filters['equipes'])

            if filters.get('tipos_servico'):
                placeholders = ','.join(['?' for _ in filters['tipos_servico']])
                query_producao += f" AND fs.descricao_servico IN ({placeholders})"
                params_producao.extend(filters['tipos_servico'])

            if filters.get('regionais'):
                placeholders = ','.join(['?' for _ in filters['regionais']])
                query_producao += f" AND fs.regional IN ({placeholders})"
                params_producao.extend(filters['regionais'])

            if filters.get('notas'):
                placeholders = ','.join(['?' for _ in filters['notas']])
                query_producao += f" AND fs.nota IN ({placeholders})"
                params_producao.extend(filters['notas'])

            if filters.get('placas'):
                placeholders = ','.join(['?' for _ in filters['placas']])
                query_producao += f" AND fs.placa IN ({placeholders})"
                params_producao.extend(filters['placas'])

        query_producao += """
        GROUP BY fs.nome_equipe, fs.cod_equipe
        ORDER BY producao_real DESC
        """

        # Query para metas
        query_meta = """
        SELECT 
            fm.nome_equipe as equipe,
            fm.cod_equipe,
            SUM(fm.valor_meta) as meta_periodo
        FROM fato_metas fm
        LEFT JOIN dim_equipes de ON fm.cod_equipe = de.cod_equipe
        LEFT JOIN dim_calendario dc ON fm.data_meta = dc.data_key
        WHERE (de.is_excluida = FALSE OR de.is_excluida IS NULL)
            AND fm.valor_meta > 0
        """
        params_meta = []

        if filters:
            if filters.get('data_inicio'):
                query_meta += " AND fm.data_meta >= ?"
                params_meta.append(filters['data_inicio'])

            if filters.get('data_fim'):
                query_meta += " AND fm.data_meta <= ?"
                params_meta.append(filters['data_fim'])

            if filters.get('equipes'):
                placeholders = ','.join(['?' for _ in filters['equipes']])
                query_meta += f" AND fm.nome_equipe IN ({placeholders})"
                params_meta.extend(filters['equipes'])

        query_meta += " GROUP BY fm.nome_equipe, fm.cod_equipe"

        try:
            with sqlite3.connect(self.db_path, timeout=15) as conn:
                df_producao = pd.read_sql_query(query_producao, conn, params=params_producao)
                df_metas = pd.read_sql_query(query_meta, conn, params=params_meta)

            if df_producao.empty:
                return pd.DataFrame()

            logger.info(f"Produção: {len(df_producao)} equipes")
            logger.info(f"Metas: {len(df_metas)} equipes no mesmo período")

            if not df_metas.empty:
                df_metas['meta_equipe'] = df_metas['meta_periodo']
                df_resultado = df_producao.merge(
                    df_metas[['equipe', 'meta_equipe']],
                    on='equipe',
                    how='left'
                )
                df_resultado['meta_equipe'] = df_resultado['meta_equipe'].fillna(0)
            else:
                df_resultado = df_producao.copy()
                df_resultado['meta_equipe'] = 0

            df_resultado['percentual_meta'] = np.where(
                df_resultado['meta_equipe'] > 0,
                (df_resultado['producao_real'] / df_resultado['meta_equipe']) * 100,
                0
            )

            return df_resultado

        except Exception as e:
            logger.error(f"Erro ao buscar dados de produção vs meta: {e}")
            return pd.DataFrame()

    def get_faturamento_data(self, filters=None):
        """Busca dados de faturamento diário com JOINs dimensionais"""
        query = """
        SELECT 
            ffd.*,
            dc.ano,
            dc.mes,
            dc.nome_mes,
            dc.nome_dia_semana,
            dc.is_fim_semana
        FROM fato_faturamento_diario ffd
        LEFT JOIN dim_calendario dc ON ffd.data = dc.data_key
        WHERE 1=1
        """
        params = []

        if filters:
            if filters.get('data_inicio'):
                query += " AND ffd.data >= ?"
                params.append(filters['data_inicio'])

            if filters.get('data_fim'):
                query += " AND ffd.data <= ?"
                params.append(filters['data_fim'])

        query += " ORDER BY ffd.data"

        try:
            with sqlite3.connect(self.db_path, timeout=15) as conn:
                return pd.read_sql_query(query, conn, params=params)
        except Exception as e:
            logger.error(f"Erro ao buscar dados de faturamento: {e}")
            return pd.DataFrame()

    def get_cache_status(self):
        """Retorna status dos caches com verificação de existência"""
        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name='cache_metadata'
                """)

                if not cursor.fetchone():
                    return pd.DataFrame(columns=['table_name', 'last_update', 'record_count', 'status'])

                df = pd.read_sql_query("""
                    SELECT table_name, last_update, record_count, status, update_duration_seconds
                    FROM cache_metadata
                    ORDER BY 
                        CASE 
                            WHEN table_name LIKE 'dim_%' THEN 1 
                            WHEN table_name LIKE 'fato_%' THEN 2 
                            ELSE 3 
                        END,
                        table_name
                """, conn)

            return df

        except Exception as e:
            logger.error(f"Erro ao obter status do cache: {e}")
            return pd.DataFrame(columns=['table_name', 'last_update', 'record_count', 'status'])


# Inicializar o gerenciador de dados com melhor controle de estado
@st.cache_resource
def get_data_manager():
    """Inicializa DataManager com controle de erro melhorado"""
    try:
        dm = DataManager()
        dm.init_cache_db()
        return dm
    except Exception as e:
        st.error(f"Erro ao conectar ao banco: {e}")
        st.stop()


def reset_data_manager():
    """Força recriação do DataManager sem loops infinitos"""
    try:
        get_data_manager.clear()
        if 'last_reset' not in st.session_state:
            st.session_state.last_reset = datetime.now()

        dm = DataManager()
        dm.init_cache_db()
        st.session_state.last_reset = datetime.now()
        return dm
    except Exception as e:
        st.error(f"Erro ao resetar sistema: {e}")
        return None


def show_cache_update_progress(data_manager):
    """Mostra progresso da atualização do cache de forma controlada"""

    # Evitar múltiplas execuções
    if 'update_in_progress' in st.session_state and st.session_state.update_in_progress:
        st.warning("Atualização já em andamento...")
        return False

    # Marcar início da atualização
    st.session_state.update_in_progress = True

    try:
        # Container para progresso
        progress_container = st.container()

        with progress_container:
            progress_bar = st.progress(0)
            status_text = st.empty()

            def progress_callback(current, total, message):
                progress = current / total
                progress_bar.progress(progress)
                status_text.text(f"{message} ({current}/{total})")

            # Executar atualização
            results = data_manager.refresh_all_caches(force=True, progress_callback=progress_callback)

            # Limpar progresso
            progress_bar.empty()
            status_text.empty()

            # Mostrar resultados
            success_count = sum(1 for r in results.values() if isinstance(r, dict) and r.get('success', False))
            total_count = len([r for r in results.values() if isinstance(r, dict)])

            if success_count == total_count:
                st.success(f"Cache atualizado com sucesso! ({success_count}/{total_count} tabelas)")
            else:
                st.warning(f"Atualização parcial: {success_count}/{total_count} tabelas atualizadas")

            # Mostrar detalhes em expander
            with st.expander("Ver detalhes da atualização"):
                for table, result in results.items():
                    if isinstance(result, dict):
                        if result['success']:
                            st.success(f"✅ {table.replace('_', ' ').title()}: {result['message']}")
                        else:
                            st.error(f"❌ {table.replace('_', ' ').title()}: {result['message']}")

            return True

    except Exception as e:
        st.error(f"Erro durante atualização: {e}")
        return False
    finally:
        # Sempre limpar o flag de atualização
        st.session_state.update_in_progress = False


def main():
    """Interface principal com controle melhorado de estado"""

    # Inicializar estados de sessão
    if 'cache_last_updated' not in st.session_state:
        st.session_state.cache_last_updated = None
    if 'update_in_progress' not in st.session_state:
        st.session_state.update_in_progress = False
    if 'data_manager_initialized' not in st.session_state:
        st.session_state.data_manager_initialized = False

    # Verificar e inicializar DataManager
    try:
        data_manager = get_data_manager()
        if not hasattr(data_manager, 'equipes_excluidas'):
            data_manager = reset_data_manager()
        st.session_state.data_manager_initialized = True
    except Exception as e:
        st.error(f"Erro ao inicializar sistema: {e}")
        st.stop()

    # Header
    st.markdown("""
    <div class="custom-header">
        <div class="logo-container">
            <img src="data:image/png;base64,{}" alt="Logo Rezende Energia">
        </div>
        <div class="text-container">
            <h1>⚡ BI Rezende Energia</h1>
            <p>Dashboard Executivo</p>
        </div>
    </div>
    """.format(get_base64_image(
        r"C:\Users\Pedro Curry\OneDrive\Área de Trabalho\Rezende\MARKETING\__sitelogo__Logo Rezende.png")),
        unsafe_allow_html=True)

    # Sidebar - Status do Cache
    st.sidebar.markdown("### 🗄️ Status do Cache Dimensional")

    # Mostrar equipes excluídas
    st.sidebar.markdown("#### ⚠️ Equipes Excluídas do Faturamento")
    equipes_excluidas_text = "<br>".join([f"• {eq}" for eq in data_manager.equipes_excluidas])
    st.sidebar.markdown(f"""
    <div style="background: #fff3cd; border: 1px solid #ffc107; border-radius: 5px; padding: 0.5rem; font-size: 0.8rem; color: #856404;">
        {equipes_excluidas_text}
    </div>
    """, unsafe_allow_html=True)

    # Status detalhado do cache
    cache_status = data_manager.get_cache_status()
    if not cache_status.empty:
        for _, row in cache_status.iterrows():
            try:
                last_update = datetime.fromisoformat(row['last_update']) if row['last_update'] else datetime.min
                time_ago = datetime.now() - last_update

                # Definir tempo de expiração baseado no tipo de tabela
                if row['table_name'].startswith('dim_'):
                    expiry_hours = 168  # 7 dias para dimensões
                else:
                    expiry_hours = data_manager.cache_duration_hours

                # Determinar status visual
                if row['status'] == 'updating':
                    status_class = "updating"
                    status_icon = "🔄"
                elif row['status'] == 'error':
                    status_class = "error"
                    status_icon = "❌"
                elif time_ago > timedelta(hours=expiry_hours):
                    status_class = "error"
                    status_icon = "⚠️"
                else:
                    status_class = ""
                    status_icon = "✅"

                # Ícone baseado no tipo de tabela
                if row['table_name'].startswith('dim_'):
                    table_icon = "📅" if "calendario" in row['table_name'] else "👥"
                else:
                    table_icon = "📊"

                table_display = row['table_name'].replace('_', ' ').title()

                duration_text = ""
                if 'update_duration_seconds' in row and row['update_duration_seconds'] and row[
                    'update_duration_seconds'] > 0:
                    duration_text = f" ({row['update_duration_seconds']}s)"

                status_text = f"{status_icon} {table_icon} {table_display}: {row['record_count']:,} registros{duration_text}"

                st.sidebar.markdown(f"""
                <div class="cache-status {status_class}">
                    {status_text}<br>
                    <small>Atualizado: {last_update.strftime('%d/%m %H:%M') if last_update != datetime.min else 'Nunca'}</small>
                </div>
                """, unsafe_allow_html=True)

            except Exception as e:
                logger.error(f"Erro ao processar status da tabela {row.get('table_name', 'unknown')}: {e}")
                continue
    else:
        st.sidebar.warning("Cache não inicializado")

    # Controles de cache - COM PROTEÇÃO CONTRA LOOPS
    st.sidebar.markdown("### 🔄 Controles de Cache")

    # Verificar se há atualização em andamento
    updating_status = st.session_state.get('update_in_progress', False)

    col1, col2 = st.sidebar.columns(2)

    with col1:
        update_button = st.button(
            "🔄 Atualizar Cache",
            type="primary",
            disabled=updating_status,
            key="update_cache_btn",
            help="Atualiza todos os dados do cache" if not updating_status else "Atualização em andamento..."
        )

        if update_button and not updating_status:
            success = show_cache_update_progress(data_manager)
            if success:
                st.session_state.cache_last_updated = datetime.now()
                # Aguardar um pouco antes de fazer rerun para evitar loops
                time.sleep(1)
                st.rerun()

    with col2:
        reset_button = st.button(
            "🔧 Reset Sistema",
            disabled=updating_status,
            key="reset_system_btn",
            help="Reinicializa o sistema completamente"
        )

        if reset_button and not updating_status:
            with st.spinner("Reiniciando sistema..."):
                data_manager = reset_data_manager()
                if data_manager:
                    st.success("Sistema reiniciado com sucesso!")
                    time.sleep(1)
                    st.rerun()

    # Auto-refresh REMOVIDO para evitar loops infinitos no cloud
    # st.sidebar.markdown("#### ⚙️ Configurações")
    # auto_refresh = st.sidebar.checkbox("Auto-refresh", value=False, help="DESABILITADO para evitar loops no cloud")

    # Health Check
    if st.sidebar.button("🔍 Verificar Conexões", key="health_check_btn"):
        with st.spinner("Verificando conexões..."):
            health = data_manager.health_check()

            for check, status in health.items():
                if status:
                    st.sidebar.success(f"✅ {check.replace('_', ' ').title()}")
                else:
                    st.sidebar.error(f"❌ {check.replace('_', ' ').title()}")

    # Filtros baseados nas dimensões
    st.sidebar.markdown("### 🎛️ Filtros Dimensionais")

    try:
        filter_options = data_manager.get_dimension_filters()
    except Exception as e:
        st.sidebar.error(f"Erro ao carregar filtros: {e}")
        filter_options = {
            'equipes': [], 'regionais': [], 'tipos_servico': [],
            'placas': [], 'notas': []
        }

    # Filtros de data
    st.sidebar.markdown("#### 📅 Período")
    col1, col2 = st.sidebar.columns(2)
    with col1:
        data_inicio = st.date_input(
            "De:",
            value=datetime.now() - timedelta(days=30),
            key="data_inicio"
        )
    with col2:
        data_fim = st.date_input(
            "Até:",
            value=datetime.now(),
            key="data_fim"
        )

    # Validação de datas
    if data_inicio > data_fim:
        st.sidebar.error("A data de início não pode ser posterior à data final!")
        return

    # Filtros de equipe
    st.sidebar.markdown("#### 👥 Equipes e Operação")
    encarregado = st.sidebar.multiselect(
        "Equipe:",
        options=filter_options.get('equipes', []),
        default=[],
        key="filter_equipes"
    )

    tipo_servico = st.sidebar.multiselect(
        "Tipo de Serviço:",
        options=filter_options.get('tipos_servico', []),
        default=[],
        key="filter_tipos_servico"
    )

    placa = st.sidebar.multiselect(
        "Placa da Equipe:",
        options=filter_options.get('placas', []),
        default=[],
        key="filter_placas"
    )

    # Filtros de localização
    st.sidebar.markdown("#### 🏢 Localização e Contrato")
    regional = st.sidebar.multiselect(
        "Regional:",
        options=filter_options.get('regionais', []),
        default=[],
        key="filter_regionais"
    )

    nota = st.sidebar.multiselect(
        "Nota/PEP:",
        options=filter_options.get('notas', []),
        default=[],
        key="filter_notas"
    )

    # Construir filtros
    filters = {
        'data_inicio': data_inicio.isoformat(),
        'data_fim': data_fim.isoformat(),
        'equipes': encarregado,
        'tipos_servico': tipo_servico,
        'placas': placa,
        'regionais': regional,
        'notas': nota
    }

    # Mostrar status de atualização se houver
    if st.session_state.get('update_in_progress', False):
        st.info("🔄 Atualização em andamento... Por favor, aguarde.")
        st.stop()

    # Carregar dados do cache dimensional
    try:
        with st.spinner("Carregando dados do modelo dimensional..."):
            df_main = data_manager.get_servicos_data(filters)
            df_meta = data_manager.get_producao_vs_meta_data(filters)
            df_faturamento = data_manager.get_faturamento_data(filters)
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        st.info("Tente atualizar o cache ou verificar as conexões.")
        return

    if df_main.empty:
        st.warning("Nenhum dado encontrado com os filtros selecionados.")
        st.info("Verifique se o cache foi inicializado e tente atualizar os dados.")

        # Sugestões quando não há dados
        with st.expander("Sugestões para resolver o problema"):
            st.markdown("""
            - Verifique se as datas estão dentro do período disponível (últimos 90 dias)
            - Remova alguns filtros para ampliar a busca
            - Clique em "Atualizar Cache" para buscar dados mais recentes
            - Verifique se há dados no Redshift para o período selecionado
            """)
        return

    # Cards de métricas principais
    st.markdown("### 📊 Indicadores Principais")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        qtd_equipes = df_main['cod_equipe'].nunique()
        delta_equipes = f"+{int(qtd_equipes * 0.05)}" if qtd_equipes > 0 else "0"
        st.metric(
            label="👥 Equipes Ativas",
            value=f"{qtd_equipes:,}",
            delta=delta_equipes
        )

    with col2:
        valor_total = df_main['valor_total'].sum()
        delta_valor = f"+R$ {valor_total * 0.08:.0f}" if valor_total > 0 else "R$ 0"
        st.metric(
            label="💰 Faturamento Total",
            value=f"R$ {valor_total:,.2f}",
            delta=delta_valor
        )

    with col3:
        total_servicos = df_main['quantidade'].sum()
        delta_servicos = f"+{int(total_servicos * 0.12)}" if total_servicos > 0 else "0"
        st.metric(
            label="🔧 Total de Serviços",
            value=f"{total_servicos:,}",
            delta=delta_servicos
        )

    with col4:
        if total_servicos > 0:
            ticket_medio = valor_total / total_servicos
            delta_ticket = f"R$ {ticket_medio * 0.03:.2f}" if ticket_medio > 0 else "R$ 0"
            st.metric(
                label="📈 Ticket Médio",
                value=f"R$ {ticket_medio:.2f}",
                delta=delta_ticket
            )
        else:
            st.metric(label="📈 Ticket Médio", value="R$ 0,00", delta="R$ 0")

    # Gráficos
    st.markdown("### 📈 Análises Visuais")

    # Gráfico Produção vs Meta
    if not df_meta.empty:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)

        periodo_texto = f"{data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}"
        total_equipes = len(df_meta)
        total_faturamento = df_meta['producao_real'].sum()
        total_meta = df_meta['meta_equipe'].sum()
        percentual_geral = (total_faturamento / total_meta * 100) if total_meta > 0 else 0

        st.markdown(f'''
        <div class="chart-title">
            🎯 Faturamento vs Meta por Equipe<br>
            <small style="font-size: 0.8em; opacity: 0.7;">
                Período: {periodo_texto} | {total_equipes} equipes<br>
                Faturamento: R$ {total_faturamento:,.2f} | Meta: R$ {total_meta:,.2f} | Atingimento: {percentual_geral:.1f}%
            </small>
        </div>
        ''', unsafe_allow_html=True)

        fig_meta = go.Figure()

        # Barras de faturamento
        fig_meta.add_trace(go.Bar(
            name='Faturamento',
            x=df_meta['equipe'],
            y=df_meta['producao_real'],
            marker_color='#F7931E',
            opacity=0.8,
            text=[f'R$ {x:,.0f}' for x in df_meta['producao_real']],
            textposition='outside',
            textfont=dict(size=11, color='#000000', family='Inter'),
        ))

        # Barras de meta se houver dados válidos
        if df_meta['meta_equipe'].sum() > 0:
            fig_meta.add_trace(go.Bar(
                name='Meta',
                x=df_meta['equipe'],
                y=df_meta['meta_equipe'],
                marker_color='#000000',
                opacity=0.6,
                text=[f'R$ {x:,.0f}' if x > 0 else 'Sem Meta' for x in df_meta['meta_equipe']],
                textposition='outside',
                textfont=dict(size=11, color='#000000', family='Inter'),
            ))

        fig_meta.update_layout(
            height=450,
            xaxis_title="Equipes",
            yaxis_title="Faturamento (R$)",
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(family="Inter", size=12),
            legend=dict(
                orientation="h",
                yanchor="top",
                y=0.98,
                xanchor="center",
                x=0.5,
                bgcolor="rgba(255,255,255,0.9)",
                bordercolor="rgba(0,0,0,0.1)",
                borderwidth=1
            ),
            margin=dict(l=70, r=30, t=80, b=100),
            barmode='group',
            xaxis=dict(tickangle=45, showgrid=False),
            yaxis=dict(
                tickformat=",.0f",
                showgrid=True,
                gridcolor='rgba(128,128,128,0.15)',
                rangemode='tozero'
            )
        )

        if not df_meta.empty:
            max_faturamento = df_meta['producao_real'].max()
            max_meta = df_meta['meta_equipe'].max() if df_meta['meta_equipe'].sum() > 0 else 0
            max_valor = max(max_faturamento, max_meta)
            fig_meta.update_yaxes(range=[0, max_valor * 1.20])

        st.plotly_chart(fig_meta, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # Gráfico de Percentual de Atingimento
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        st.markdown('<div class="chart-title">📊 Percentual de Atingimento da Meta por Equipe</div>',
                    unsafe_allow_html=True)

        fig_percentual = go.Figure()

        colors = ['#4CAF50' if x >= 100 else '#FF9800' if x >= 80 else '#F44336' for x in df_meta['percentual_meta']]

        fig_percentual.add_trace(go.Bar(
            x=df_meta['equipe'],
            y=df_meta['percentual_meta'],
            marker_color=colors,
            text=[f'{x:.1f}%' for x in df_meta['percentual_meta']],
            textposition='outside',
            textfont=dict(size=11, color='#000000', family='Inter'),
            showlegend=False
        ))

        fig_percentual.add_hline(
            y=100,
            line_dash="dash",
            line_color="red",
            opacity=0.7,
            annotation_text="Meta 100%",
            annotation_position="top right"
        )

        fig_percentual.update_layout(
            height=350,
            xaxis_title="Equipes",
            yaxis_title="% de Atingimento da Meta",
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(family="Inter", size=12),
            margin=dict(l=70, r=30, t=60, b=100),
            xaxis=dict(tickangle=45, showgrid=False),
            yaxis=dict(
                tickformat=".0f",
                showgrid=True,
                gridcolor='rgba(128,128,128,0.15)',
                rangemode='tozero'
            )
        )

        if not df_meta.empty:
            max_percentual = df_meta['percentual_meta'].max()
            fig_percentual.update_yaxes(range=[0, max(max_percentual * 1.15, 120)])

        st.plotly_chart(fig_percentual, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # Gráfico de Faturamento Diário
    if not df_faturamento.empty:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)

        periodo_texto = f"{data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}"
        st.markdown(
            f'<div class="chart-title">💰 Faturamento Diário<br><small style="font-size: 0.8em; opacity: 0.7;">Período: {periodo_texto}</small></div>',
            unsafe_allow_html=True)

        fig_fat = px.bar(
            df_faturamento,
            x='data',
            y='faturamento_diario',
            title="",
            color='faturamento_diario',
            color_continuous_scale=['#000000', '#F7931E'],
            text='faturamento_diario'
        )

        fig_fat.update_traces(
            texttemplate='R$ %{text:,.0f}',
            textposition='outside',
            textfont=dict(size=12, color='#000000', family='Inter', weight='bold')
        )

        fig_fat.update_layout(
            height=400,
            xaxis_title="Data",
            yaxis_title="Faturamento (R$)",
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(family="Inter", size=12),
            showlegend=False,
            xaxis=dict(tickangle=45),
            yaxis=dict(
                tickformat=",.0f",
                showgrid=True,
                gridcolor='rgba(128,128,128,0.15)',
                rangemode='tozero'
            ),
            margin=dict(l=80, r=20, t=60, b=120)
        )

        if not df_faturamento.empty:
            max_value = df_faturamento['faturamento_diario'].max()
            fig_fat.update_yaxes(range=[0, max_value * 1.15])

        st.plotly_chart(fig_fat, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # Gráfico Top Equipes
    if not df_main.empty:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        st.markdown('<div class="chart-title">🏆 Top 10 Equipes por Faturamento</div>', unsafe_allow_html=True)

        equipes_top = df_main.groupby('nome_equipe').agg({
            'valor_total': 'sum',
            'quantidade': 'sum'
        }).reset_index().sort_values('valor_total', ascending=False).head(10)

        fig_top = px.bar(
            equipes_top,
            x='nome_equipe',
            y='valor_total',
            title="",
            color='valor_total',
            color_continuous_scale=['#000000', '#F7931E'],
            text='valor_total'
        )

        fig_top.update_traces(
            texttemplate='R$ %{text:,.0f}',
            textposition='outside',
            textfont=dict(size=12, color='#000000', family='Inter', weight='bold')
        )

        fig_top.update_layout(
            height=400,
            xaxis_title="Equipes",
            yaxis_title="Faturamento Total (R$)",
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(family="Inter", size=12),
            showlegend=False,
            xaxis=dict(tickangle=45),
            yaxis=dict(
                tickformat=",.0f",
                showgrid=True,
                gridcolor='rgba(128,128,128,0.15)',
                rangemode='tozero'
            ),
            margin=dict(l=80, r=20, t=60, b=120)
        )

        if not equipes_top.empty:
            max_value = equipes_top['valor_total'].max()
            fig_top.update_yaxes(range=[0, max_value * 1.15])

        st.plotly_chart(fig_top, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # Tabela detalhada com melhor controle
    st.markdown("### 📋 Tabela Detalhada de Operações")

    # Preparar dados da tabela
    try:
        df_table = df_main.copy()
        df_table['valor_unitario'] = df_table['valor_unitario'].round(2)
        df_table['valor_total'] = df_table['valor_total'].round(2)

        # Renomear colunas para exibição
        df_table_display = df_table[[
            'nome_equipe', 'descricao_servico', 'quantidade', 'valor_unitario', 'valor_total',
            'regional', 'nota', 'placa', 'cidade', 'data_execucao'
        ]].copy()

        df_table_display.columns = [
            'Equipe', 'Descrição do Serviço', 'Quantidade', 'Valor Unitário (R$)', 'Valor Total (R$)',
            'Regional', 'Nota', 'Placa', 'Cidade', 'Data'
        ]

        # Formatação da tabela
        def format_currency(val):
            return f"R$ {val:,.2f}"

        def format_date(val):
            if pd.isna(val):
                return "N/A"
            try:
                return pd.to_datetime(val).strftime("%d/%m/%Y")
            except:
                return str(val)

        # Aplicar formatações
        df_table_display['Valor Unitário (R$)'] = df_table_display['Valor Unitário (R$)'].apply(format_currency)
        df_table_display['Valor Total (R$)'] = df_table_display['Valor Total (R$)'].apply(format_currency)
        df_table_display['Data'] = df_table_display['Data'].apply(format_date)

        # Controles da tabela
        col1, col2, col3 = st.columns([2, 1, 1])

        with col1:
            search_term = st.text_input("🔍 Buscar na tabela:", placeholder="Digite para filtrar...", key="table_search")

        with col2:
            page_size = st.selectbox("Registros por página:", [10, 25, 50, 100], index=1, key="page_size")

        with col3:
            sort_column = st.selectbox("Ordenar por:", df_table_display.columns.tolist(), key="sort_column")

        # Filtrar tabela com busca
        if search_term:
            mask = df_table_display.astype(str).apply(lambda x: x.str.contains(search_term, case=False, na=False)).any(
                axis=1)
            df_filtered = df_table_display[mask]
        else:
            df_filtered = df_table_display.copy()

        # Ordenar
        try:
            if sort_column in ['Valor Unitário (R$)', 'Valor Total (R$)']:
                orig_col = 'valor_unitario' if 'Unitário' in sort_column else 'valor_total'
                sort_values = df_table[orig_col].loc[df_filtered.index]
                df_filtered = df_filtered.iloc[sort_values.argsort()[::-1]]
            else:
                df_filtered = df_filtered.sort_values(sort_column)
        except Exception as e:
            logger.warning(f"Erro na ordenação: {e}")

        # Paginação
        total_pages = len(df_filtered) // page_size + (1 if len(df_filtered) % page_size > 0 else 0)
        if total_pages > 0:
            page = st.selectbox(f"Página (Total: {total_pages}):", range(1, total_pages + 1), key="current_page")
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            df_page = df_filtered.iloc[start_idx:end_idx]
        else:
            df_page = df_filtered
            page = 1

        # Configurar colunas da tabela
        column_config = {
            "Equipe": st.column_config.TextColumn("Equipe", width="medium"),
            "Descrição do Serviço": st.column_config.TextColumn("Descrição do Serviço", width="large"),
            "Quantidade": st.column_config.NumberColumn("Quantidade", format="%d", width="small"),
            "Valor Unitário (R$)": st.column_config.TextColumn("Valor Unitário (R$)", width="medium"),
            "Valor Total (R$)": st.column_config.TextColumn("Valor Total (R$)", width="medium"),
            "Regional": st.column_config.TextColumn("Regional", width="medium"),
            "Nota": st.column_config.TextColumn("Nota", width="small"),
            "Placa": st.column_config.TextColumn("Placa", width="small"),
            "Cidade": st.column_config.TextColumn("Cidade", width="medium"),
            "Data": st.column_config.TextColumn("Data", width="small")
        }

        # Exibir tabela
        st.dataframe(
            df_page,
            column_config=column_config,
            use_container_width=True,
            hide_index=True,
            height=400
        )

        # Informações da tabela
        if not df_filtered.empty:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.info(f"📊 **Total de registros:** {len(df_filtered):,}")
            with col2:
                st.info(f"📄 **Página atual:** {page} de {total_pages if total_pages > 0 else 1}")
            with col3:
                st.info(f"💾 **Registros na página:** {len(df_page):,}")

            # Estatísticas adicionais
            st.markdown("#### 📈 Estatísticas dos Dados Filtrados")

            stats_col1, stats_col2, stats_col3, stats_col4 = st.columns(4)

            with stats_col1:
                equipes_filtradas = df_filtered['Equipe'].nunique()
                st.metric("Equipes Únicas", equipes_filtradas)

            with stats_col2:
                valor_medio_filtrado = df_main.loc[
                    df_filtered.index if len(df_filtered) <= len(df_main) else df_main.index, 'valor_total'].mean()
                st.metric("Valor Médio", f"R$ {valor_medio_filtrado:.2f}")

            with stats_col3:
                servicos_filtrados = df_main.loc[
                    df_filtered.index if len(df_filtered) <= len(df_main) else df_main.index, 'quantidade'].sum()
                st.metric("Serviços Filtrados", f"{servicos_filtrados:,}")

            with stats_col4:
                regionais_filtradas = df_filtered['Regional'].nunique()
                st.metric("Regionais Ativas", regionais_filtradas)

            # Download da tabela
            csv = df_filtered.to_csv(index=False)
            st.download_button(
                label="📥 Baixar Tabela Completa (CSV)",
                data=csv,
                file_name=f"rezende_energia_operacoes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                key="download_csv"
            )
        else:
            st.warning("Nenhum dado encontrado com os filtros aplicados.")

    except Exception as e:
        st.error(f"Erro ao processar tabela: {e}")

    # Debug/Status avançado
    with st.expander("🔍 Informações Técnicas do Sistema"):
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**📊 Dados Carregados:**")
            if not df_main.empty:
                data_min = df_main['data_execucao'].min()
                data_max = df_main['data_execucao'].max()
                st.write(f"• Registros: {len(df_main):,}")
                st.write(f"• Período: {data_min} a {data_max}")
                st.write(f"• Equipes: {df_main['cod_equipe'].nunique()}")

                total_kpi = df_main['valor_total'].sum()
                total_grafico = df_meta['producao_real'].sum() if not df_meta.empty else 0
                diferenca = abs(total_kpi - total_grafico)
                consistencia = "✅ Consistente" if diferenca < 1 else "⚠️ Diferença detectada"
                st.write(f"• Consistência: {consistencia}")

                if not df_meta.empty:
                    equipes_com_meta = df_meta[df_meta['meta_equipe'] > 0][
                        'equipe'].nunique() if 'meta_equipe' in df_meta.columns else 0
                    st.write(f"• Equipes c/ Meta: {equipes_com_meta}")
            else:
                st.write("• Nenhum dado carregado")

        with col2:
            st.markdown("**🎛️ Filtros Aplicados:**")
            filtros_ativos = []
            if filters.get('data_inicio') or filters.get('data_fim'):
                filtros_ativos.append(f"📅 Data: {data_inicio.strftime('%d/%m')} - {data_fim.strftime('%d/%m')}")
            if filters.get('equipes'):
                filtros_ativos.append(f"👥 Equipes: {len(filters['equipes'])}")
            if filters.get('tipos_servico'):
                filtros_ativos.append(f"🔧 Serviços: {len(filters['tipos_servico'])}")
            if filters.get('placas'):
                filtros_ativos.append(f"🚗 Placas: {len(filters['placas'])}")
            if filters.get('regionais'):
                filtros_ativos.append(f"🏢 Regionais: {len(filters['regionais'])}")
            if filters.get('notas'):
                filtros_ativos.append(f"📋 Notas: {len(filters['notas'])}")

            if filtros_ativos:
                for filtro in filtros_ativos:
                    st.write(f"• {filtro}")
            else:
                st.write("• Nenhum filtro ativo")

    # Status detalhado do cache dimensional
    st.markdown("---")

    with st.expander("📊 Status Detalhado do Cache Dimensional"):
        cache_status = data_manager.get_cache_status()
        if not cache_status.empty:
            cache_display = cache_status.copy()
            cache_display['Tipo'] = cache_display['table_name'].apply(lambda x:
                                                                      "📅 Dimensão" if x.startswith(
                                                                          'dim_') else "📊 Fato")
            cache_display['Tabela'] = cache_display['table_name'].apply(lambda x:
                                                                        x.replace('_', ' ').title())
            cache_display['Registros'] = cache_display['record_count'].apply(lambda x: f"{x:,}")
            cache_display['Última Atualização'] = cache_display['last_update'].apply(lambda x:
                                                                                     datetime.fromisoformat(x).strftime(
                                                                                         '%d/%m/%Y %H:%M') if x else 'Nunca')
            cache_display['Duração (s)'] = cache_display['update_duration_seconds'].fillna(0).apply(
                lambda x: f"{int(x)}s" if x > 0 else "-")

            st.dataframe(
                cache_display[['Tipo', 'Tabela', 'Registros', 'Última Atualização', 'Duração (s)', 'status']],
                column_config={
                    "Tipo": "Tipo",
                    "Tabela": "Tabela",
                    "Registros": "Registros",
                    "Última Atualização": "Última Atualização",
                    "Duração (s)": "Duração",
                    "status": "Status"
                },
                hide_index=True,
                use_container_width=True
            )

        # Informações do banco SQLite
        try:
            db_size = os.path.getsize(data_manager.db_path) / (1024 * 1024)
        except:
            db_size = 0

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Tamanho do Cache", f"{db_size:.2f} MB")
        with col2:
            total_tables = len(cache_status) if not cache_status.empty else 0
            st.metric("Tabelas no Cache", total_tables)
        with col3:
            total_records = cache_status['record_count'].sum() if not cache_status.empty else 0
            st.metric("Total de Registros", f"{total_records:,}")
        with col4:
            update_time = st.session_state.get('cache_last_updated')
            if update_time:
                last_update_text = update_time.strftime('%H:%M')
            else:
                last_update_text = "Nunca"
            st.metric("Última Sincronização", last_update_text)

    # Footer
    st.markdown(
        "<div style='text-align: center; color: #6c757d; font-size: 0.9rem; margin-top: 2rem;'>"
        "⚡ <b>BI - Rezende Energia</b><br>"
        f"🕒 Dashboard atualizado: {datetime.now().strftime('%d/%m/%Y às %H:%M')}<br>"
        f"🔄 Cache inicializado: {'✅' if st.session_state.get('data_manager_initialized', False) else '❌'}"
        "</div>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"Erro crítico na aplicação: {e}")
        st.info("Tente recarregar a página ou contactar o suporte técnico.")
        logger.error(f"Erro crítico: {e}")
