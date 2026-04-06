const STATUS_LABELS = {
  searching:   'Exa AI’da ilan aranıyor...',
  scraping:    'İlanlar scrape ediliyor...',
  merging:     'Sonuçlar birleştiriliyor...',
  completed:   'Tamamlandı!',
  failed:      'Hata oluştu',
};

export default function StatusBar({ status }) {
  return (
    <div className="status-bar">
      <div className="loading-spinner" />
      <p>{STATUS_LABELS[status] || status}</p>
    </div>
  );
}
