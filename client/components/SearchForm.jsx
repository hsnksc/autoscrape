import { useState } from 'react';

export default function SearchForm({ onSubmit, disabled }) {
  const [location, setLocation] = useState('');
  const [rooms, setRooms] = useState('');
  const [minPrice, setMinPrice] = useState('');
  const [maxPrice, setMaxPrice] = useState('');

  const handleSubmit = (e) => {
    e.preventDefault();
    onSubmit({
      location,
      rooms: rooms || undefined,
      minPrice: minPrice ? parseInt(minPrice) : undefined,
      maxPrice: maxPrice ? parseInt(maxPrice) : undefined,
    });
  };

  return (
    <form className="search-form" onSubmit={handleSubmit}>
      <div>
        <label>Konum</label>
        <input
          type="text"
          placeholder="Kadıköy, Beşiktaş..."
          value={location}
          onChange={(e) => setLocation(e.target.value)}
          required
        />
      </div>

      <div>
        <label>Oda Sayısı</label>
        <select value={rooms} onChange={(e) => setRooms(e.target.value)}>
          <option value="">Tümü</option>
          <option value="1+0">1+0</option>
          <option value="1+1">1+1</option>
          <option value="2+1">2+1</option>
          <option value="3+1">3+1</option>
          <option value="4+1">4+1</option>
          <option value="5+">5+</option>
        </select>
      </div>

      <div>
        <label>Min Fiyat (TL)</label>
        <input
          type="number"
          placeholder="1.000.000"
          value={minPrice}
          onChange={(e) => setMinPrice(e.target.value)}
        />
      </div>

      <div>
        <label>Max Fiyat (TL)</label>
        <input
          type="number"
          placeholder="5.000.000"
          value={maxPrice}
          onChange={(e) => setMaxPrice(e.target.value)}
        />
      </div>

      <button className="btn-search" type="submit" disabled={disabled}>
        {disabled ? 'Araniyor...' : 'Ara'}
      </button>
    </form>
  );
}