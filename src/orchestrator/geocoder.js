/**
 * geocoder.js — Google Maps Geocoding API
 *
 * lat/lng koordinatını il / ilçe / mahalle + yakın mahalleler olarak çevirir.
 * Yakın mahalleler için merkez + 4 yön noktası (1 km) paralel geocode edilir.
 */

const GEOCODE_URL = 'https://maps.googleapis.com/maps/api/geocode/json';

const FALLBACK = { location: '', mahalle: '', ilce: '', il: '', nearbyMahalleler: [] };

export class Geocoder {
  #apiKey;
  #logger;

  constructor() {
    this.#apiKey = process.env.GOOGLE_MAPS_API_KEY;
    this.#logger = null;
  }

  setLogger(logger) {
    this.#logger = logger;
  }

  /**
   * Koordinatı il/ilçe/mahalle + yakın mahalleler + komşu ilçelere çevirir.
   *
   * İki halka:
   *   İç halka (1 km, 4 + merkez = 5 nokta): komşu mahalleleri keşfeder
   *   Dış halka (3 km, 8 nokta): komşu ilçeleri keşfeder
   *
   * @param {number} lat
   * @param {number} lng
   * @returns {Promise<{location, mahalle, ilce, il, nearbyMahalleler, nearbyIlceler}>}
   */
  async reverseGeocode(lat, lng) {
    if (!this.#apiKey) {
      this.#logger?.warn('GOOGLE_MAPS_API_KEY eksik — koordinat ham kullanılacak');
      return { ...FALLBACK, location: `${lat},${lng}` };
    }

    const latPer1km = 1 / 111.32;
    const lngPer1km = 1 / (111.32 * Math.cos(lat * Math.PI / 180));

    // İç halka: 1 km — mahalle keşfi (merkez + K/G/D/B)
    const innerPoints = [
      [lat, lng],
      [lat + latPer1km, lng],
      [lat - latPer1km, lng],
      [lat, lng + lngPer1km],
      [lat, lng - lngPer1km],
    ];

    // Dış halka: 3 km — ilçe keşfi (8 yön: K/KD/D/GD/G/GB/B/KB)
    const r3lat = 3 * latPer1km;
    const r3lng = 3 * lngPer1km;
    const outerPoints = [
      [lat + r3lat, lng],
      [lat + r3lat, lng + r3lng],
      [lat, lng + r3lng],
      [lat - r3lat, lng + r3lng],
      [lat - r3lat, lng],
      [lat - r3lat, lng - r3lng],
      [lat, lng - r3lng],
      [lat + r3lat, lng - r3lng],
    ];

    // Tüm noktaları paralel geocode et
    const [innerResults, outerResults] = await Promise.all([
      Promise.all(innerPoints.map(([la, lo]) => this.#geocodeSingle(la, lo).catch(() => null))),
      Promise.all(outerPoints.map(([la, lo]) => this.#geocodeSingle(la, lo).catch(() => null))),
    ]);

    const main = innerResults[0];
    if (!main) {
      this.#logger?.warn({ lat, lng }, 'Geocode merkez sonuç yok');
      return { ...FALLBACK, location: `${lat},${lng}` };
    }

    // Komşu mahalleler (iç halkadan, benzersiz)
    const nearbyMahalleler = [...new Set(
      innerResults.filter(Boolean).map((r) => r.mahalle).filter(Boolean),
    )];

    // Komşu ilçeler: ana ilçe + dış halkadan gelen tüm ilçeler (benzersiz)
    const outerIlceler = outerResults.filter(Boolean).map((r) => r.ilce).filter(Boolean);
    const nearbyIlceler = [...new Set([main.ilce, ...outerIlceler].filter(Boolean))];

    const locationParts = [main.mahalle, main.ilce, main.il].filter(Boolean);
    const location = locationParts.join(', ') || `${lat},${lng}`;

    this.#logger?.info(
      { lat, lng, location, nearbyMahalleler, nearbyIlceler },
      'Geocode tamamlandı',
    );

    return {
      location,
      mahalle: main.mahalle,
      ilce: main.ilce,
      il: main.il,
      nearbyMahalleler,
      nearbyIlceler,
      district: main.ilce,
      city: main.il,
    };
  }

  /**
   * Tek bir koordinat noktasını geocode eder.
   */
  async #geocodeSingle(lat, lng) {
    const url = `${GEOCODE_URL}?latlng=${lat},${lng}&language=tr&key=${this.#apiKey}`;
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(6000) });
      if (!res.ok) return null;
      const data = await res.json();
      if (data.status !== 'OK' || !data.results?.length) return null;
      return this.#extractLocation(data.results);
    } catch {
      return null;
    }
  }

  /**
   * Google address_components'tan il / ilçe / mahalle çıkarır.
   *
   * Türkiye için Google Maps tipleri:
   *   administrative_area_level_4 → mahalle
   *   administrative_area_level_3 → bucak (nadiren)
   *   administrative_area_level_2 → ilçe
   *   administrative_area_level_1 → il
   *   locality                    → büyük şehir adı (İstanbul, Ankara…)
   *   sublocality_level_1/2       → alternatif mahalle/semt
   *   neighborhood                → semt/mahalle (eski fotoğraf tabanlı)
   */
  #extractLocation(results) {
    const allComponents = results.flatMap((r) => r.address_components || []);

    const pick = (...types) => {
      for (const t of types) {
        const found = allComponents.find((c) => c.types.includes(t))?.long_name;
        if (found) return found;
      }
      return '';
    };

    // Mahalle: level_4 > neighborhood > sublocality_level_2 > sublocality_level_1
    const mahalle = pick(
      'administrative_area_level_4',
      'neighborhood',
      'sublocality_level_2',
      'sublocality_level_1',
    );

    // İlçe: level_2 > sublocality_level_1 > locality (küçük şehirlerde level_1)
    const ilce = pick(
      'administrative_area_level_2',
      'sublocality_level_1',
    );

    // İl: locality > level_1
    const il = pick('locality', 'administrative_area_level_1');

    return { mahalle, ilce, il };
  }
}
