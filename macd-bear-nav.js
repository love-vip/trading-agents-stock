// Additive navigation only. The bearish page uses the original page template
// with independent member/history endpoints; the normal MACD flow is untouched.
(() => {
  const bearish = location.pathname === '/macd-bear';
  let count = null;
  const mount = () => {
    const anchor = document.getElementById('macdScreenerNav');
    if (!anchor || document.getElementById('macdBearNav')) return;
    const button = document.createElement('button');
    button.id = 'macdBearNav'; button.type = 'button';
    button.className = 'nav-button' + (bearish && state.pageView === 'macd-screener' ? ' active' : '');
    button.innerHTML = '<span>MACD空头</span><span class="count nav-count menu-quantity-badge whitelist-market-count">' + (count ?? '--') + '</span>';
    if (bearish) {
      anchor.classList.remove('active');
      anchor.querySelector('span').textContent = 'MACD选股';
      anchor.querySelector('.nav-count')?.remove();
      anchor.addEventListener('click', event => {
        event.stopImmediatePropagation(); location.href = '/';
      }, true);
    }
    button.addEventListener('click', () => { location.href = '/macd-bear'; });
    anchor.after(button);
  };
  const nav = document.getElementById('sectorNav');
  if (nav) new MutationObserver(mount).observe(nav, {childList: true});
  mount();
  async function updateCount() {
    try {
      const response = await fetch('/api/macd-bear/members', {cache:'no-store'});
      if (!response.ok) return;
      const data = await response.json(); count = (data.stocks || []).length;
      const badge = document.querySelector('#macdBearNav .count');
      if (badge) badge.textContent = count;
      if (bearish) {
        let status = document.getElementById('macdBearStatus');
        if (!status) {
          status = document.createElement('p'); status.id = 'macdBearStatus';
          document.getElementById('whitelistTitle')?.after(status);
        }
        const error = data.scanStatus?.lastError;
        status.hidden = !error;
        status.textContent = error ? '空头扫描异常，当前保留上次结果：' + error : '';
      }
    } catch (error) { console.warn('MACD空头数量读取失败', error); }
  }
  updateCount(); setInterval(updateCount, 60000);
})();
