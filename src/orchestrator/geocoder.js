/**
 * geocoder.js — Google Maps Geocoding API
 *
 * lat/lng koordinatını mahalle + ilçe adına çevirir.
 * Çıktı Exa AI sorgusuna eklenir: "Moda Kadıköy 2+1 ... TL ilan"
 */

const GEOCODE_URL = 'https://maps.googleapis.com/maps/api/geocode/json';

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
   * Koordinatı okunabilir konum metnine çevirir.
   * @param {number} lat
   * @param {number} lng
   * @returns {Promise<string>}  örn. "Moda, Kadıköy" veya "Kadıköy, İstanbul"
   */
  async reverseGeocode(lat, lng) {
    if (!this.#apiKey) {
      this.#logger?.warn('GOOGLE_MAPS_API_KEY eksik — koordinat ham olarak kullanılacak');
      return { location: `${lat},${lng}`, district: '' };
    }

    const url = `${GEOCODE_URL}?latlng=${lat},${lng}&language=tr&key=${this.#apiKey}`;

    try {
      const res = await fetch(url);
      if (!res.ok) throw new Error(`Google Maps HTTP ${res.status}`);

      const data = await res.json();

      if (data.status !== 'OK' || !data.results?.length) {
        this.#logger?.warn({ status: data.status }, 'Geocode sonuç yok');
        return { location: `${lat},${lng}`, district: '' };
      }

      const result = this.#extractLocation(data.results);
      this.#logger?.info({ lat, lng, location: result.location }, 'Geocode OK');
      return result;
    } catch (err) {
      this.#logger?.error({ err }, 'Geocode hatası');
      return { location: `${lat},${lng}`, district: '' };
    }
  }

  /**
   * Google results dizisinden mahalle + ilçe çıkarır.
   * Bileşen tipleri öncelik sırasıyla: neighborhood > sublocality → locality
   */
  #extractLocation(results) {
    // Tüm address_components'ı tek düz listede topla
    const allComponents = results.flatMap((r) => r.address_components || []);

    const pick = (type) =>
      allComponents.find((c) => c.types.includes(type))?.long_name || '';

    const neighborhood = pick('neighborhood') || pick('sublocality_level_2') || pick('sublocality_level_1');
    const district     = pick('sublocality_level_1') || pick('administrative_area_level_2') || pick('locality');
    const city         = pick('locality') || pick('administrative_area_level_1');

    const parts = [neighborhood, district !== neighborhood ? district : '']
      .filter(Boolean);

    // Fallback: sadece şehir varsa onu kullan
    const locationStr = parts.length
      ? parts.join(', ')
      : (city || results[0]?.formatted_address || '');

    return { location: locationStr, district };
  }
}
