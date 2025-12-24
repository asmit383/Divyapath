import logging
import mysql.connector
from datetime import datetime
from mysql.connector import errorcode
from typing import List, Dict, Optional, Any

from core.config import settings

logger = logging.getLogger(__name__)

def get_connection():
    """
    Establishes and returns a new MySQL database connection.
    Uses credentials from application settings.
    """
    try:
        cnx = mysql.connector.connect(
            host=settings.MYSQL_HOST,
            port=settings.MYSQL_PORT,
            user=settings.MYSQL_USER,
            password=settings.MYSQL_PASSWORD,
            database=settings.MYSQL_DATABASE
        )
        return cnx
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logger.error("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logger.error("Database does not exist")
        else:
            logger.error(f"Failed to connect to MySQL: {err}")
        raise

def insert_sighting_history(data: Dict[str, Any]) -> None:
    """
    Inserts a historical sighting record into MySQL.
    
    Args:
        data: Dictionary containing sighting details 
              (idol_id, lat, lon, rssi, timestamp, volunteer_id).
    """
    cnx = None
    cursor = None
    query = ("INSERT INTO sightings "
             "(idol_id, lat, lon, rssi, timestamp, volunteer_id) "
             "VALUES (%s, %s, %s, %s, %s, %s)")

    try:
        cnx = get_connection()
        cursor = cnx.cursor()
        
        # Ensure timestamp is in correct format if it's a string, 
        # but mysql-connector handles datetime objects well.
        
        params = (
            data.get('idol_id'),
            data.get('lat'),
            data.get('lon'),
            data.get('rssi'),
            data.get('timestamp'),
            data.get('volunteer_id')
        )
        
        cursor.execute(query, params)
        cnx.commit()
    except mysql.connector.Error as err:
        logger.error(f"Failed to insert sighting history: {err}")
        if cnx:
            cnx.rollback()
    finally:
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()

def get_latest_positions(limit: int = 100) -> List[Dict[str, Any]]:
    """
    Retrieves the most recent sighting for each idol (conceptually), 
    or simply the last N records strictly by time.
    Note: For true 'latest per idol', a more complex query is needed.
    This function implements a simple 'latest global events' for admin feeds.
    """
    cnx = None
    cursor = None
    query = ("SELECT idol_id, lat, lon, rssi, timestamp, volunteer_id "
             "FROM sightings "
             "ORDER BY timestamp DESC LIMIT %s")
    results = []
    
    try:
        cnx = get_connection()
        cursor = cnx.cursor(dictionary=True)
        cursor.execute(query, (limit,))
        results = cursor.fetchall()
        return results
    except mysql.connector.Error as err:
        logger.error(f"Failed to fetch latest positions: {err}")
        return []
    finally:
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()

def get_sighting_history(idol_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Retrieves the historical path for a specific idol.
    """
    cnx = None
    cursor = None
    query = ("SELECT idol_id, lat, lon, rssi, timestamp, volunteer_id "
             "FROM sightings "
             "WHERE idol_id = %s "
             "ORDER BY timestamp DESC LIMIT %s")
    results = []
    
    try:
        cnx = get_connection()
        cursor = cnx.cursor(dictionary=True)
        cursor.execute(query, (idol_id, limit))
        results = cursor.fetchall()
        return results
    except mysql.connector.Error as err:
        logger.error(f"Failed to fetch history for idol {idol_id}: {err}")
        return []
    finally:
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()
