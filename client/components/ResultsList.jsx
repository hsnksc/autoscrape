const currencyFormat = new Intl.NumberFormat('tr-TR', {
  style: 'currency',
  currency: 'TRY',
  maximumFractionDigits: 0,
});

export default function ResultsList({ items }) {
  return (
    <div className="results">
      <h2>{items.length} ilan bulundu</h2>
      {items.map((item, i) => (
        <a
          key={item.url || i}
          href={item.url}
          target="_blank"
          rel="noopener noreferrer"
          className="result-card"
        >
          <div className="img-placeholder">
            {item.images?.[0] && <img src={item.images[0]} alt="" />}
          </div>
          <div className="info">
            <div className="title">{item.title}</div>
            {item.price && (
              <div className="price">{currencyFormat.format(item.price)}</div>
            )}
            <div className="meta">
              {item.city} / {item.district}
              {item.rooms && ` · ${item.rooms}`}
              {item.netM2 && ` · ${item.netM2} m²`}
            </div>
            <div>
              <span className="badge">{item.domain}</span>
              {item.source === 'cache' && (
                <span className="badge" style={{ marginLeft: 6 }}>cache</span>
              )}
            </div>
          </div>
        </a>
      ))}
    </div>
  );
}