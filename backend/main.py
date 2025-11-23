from fastapi import FastAPI
import swisseph as swe
from pydantic import BaseModel
from datetime import datetime

app = FastAPI(title="SomaClarity Intelligence Engine")

# Set the Ephemeris Path (Download the ephe files later for high precision)
# swe.set_ephe_path('/usr/share/libswe/ephe') 

class BirthData(BaseModel):
    year: int
    month: int
    day: int
    hour: float
    lat: float
    lon: float

@app.get("/")
def health_check():
    return {"status": "SomaClarity is breathing", "timestamp": datetime.now()}

@app.post("/analyze/sun")
def get_sun_position(data: BirthData):
    # 1. Convert Date to Julian Day (The Universal Time format)
    julian_day = swe.julday(data.year, data.month, data.day, data.hour)
    
    # 2. Calculate Sun Position (Planet ID 0 = Sun)
    # returns: ((longitude, latitude, distance, speed, etc), error)
    position, flags = swe.calc_ut(julian_day, 0)
    
    # 3. Logic: Map Degrees to Zodiac Sign
    degree = position[0]
    zodiac_index = int(degree / 30)
    zodiac_signs = ["Aries", "Taurus", "Gemini", "Cancer", "Leo", "Virgo", 
                    "Libra", "Scorpio", "Sagittarius", "Capricorn", "Aquarius", "Pisces"]
    
    return {
        "julian_day": julian_day,
        "sun_longitude_deg": round(degree, 2),
        "zodiac_sign": zodiac_signs[zodiac_index],
        "energy_profile": "High" if zodiac_index == 0 else "Variable" # Simple Logic
    }