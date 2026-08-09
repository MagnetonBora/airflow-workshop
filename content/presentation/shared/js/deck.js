// Deck.js - Presentation controller

const deck = document.querySelector('.deck');
const slides = [...document.querySelectorAll('.slide')];
const N = slides.length;
const dotsEl = document.getElementById('dots');
const ctrEl = document.getElementById('slide-counter');
const progressBar = document.getElementById('progress-bar');
const slideIds = slides.map((slide) => slide.id);

const dots = slides.map((_, i) => {
  const dot = document.createElement('div');
  dot.className = 'dot' + (i === 0 ? ' on' : '');
  dot.onclick = () => goTo(i);
  dotsEl.appendChild(dot);
  return dot;
});

let current = 0;
let scrolling = false;
let scrollTimer = null;
let ignoreHashChange = false;

function indexFromHash(hashValue) {
  const raw = (hashValue || '').replace(/^#/, '');
  if (!raw) return -1;
  return slideIds.indexOf(raw);
}

function syncHash(index) {
  const id = slideIds[index];
  if (!id) return;
  const nextHash = '#' + id;
  if (window.location.hash === nextHash) return;

  ignoreHashChange = true;
  history.replaceState(null, '', nextHash);
  setTimeout(() => { ignoreHashChange = false; }, 0);
}

function setUI(index, shouldSyncHash = false) {
  current = index;
  dots.forEach((d, i) => d.classList.toggle('on', i === index));
  ctrEl.textContent = (index + 1) + ' / ' + N;
  progressBar.style.width = ((index + 1) / N * 100) + '%';
  if (shouldSyncHash) syncHash(index);
}

function goTo(index, options = {}) {
  const { behavior = 'smooth', shouldSyncHash = true } = options;
  if (index < 0 || index >= N) return;
  current = index;
  scrolling = true;

  const targetSlide = slides[index];
  if (targetSlide) {
    deck.scrollTo({
      left: targetSlide.offsetLeft,
      behavior
    });
  }
  setUI(index, shouldSyncHash);

  // Unlock after animation
  clearTimeout(scrollTimer);
  scrollTimer = setTimeout(() => { scrolling = false; }, 600);
}

// Update counter on manual scroll (drag/trackpad)
deck.addEventListener('scroll', () => {
  if (scrolling) return;
  clearTimeout(scrollTimer);
  scrollTimer = setTimeout(() => {
    const deckLeft = deck.getBoundingClientRect().left;
    let closest = 0, minDist = Infinity;
    slides.forEach((slide, i) => {
      const dist = Math.abs(slide.getBoundingClientRect().left - deckLeft);
      if (dist < minDist) { minDist = dist; closest = i; }
    });
    setUI(closest, true);
  }, 80);
});

document.addEventListener('keydown', e => {
  if (e.key === 'ArrowRight' || e.key === ' ') { e.preventDefault(); goTo(current + 1); }
  else if (e.key === 'ArrowLeft')               { e.preventDefault(); goTo(current - 1); }
});

let touchX = 0;
document.addEventListener('touchstart', e => { touchX = e.touches[0].clientX; });
document.addEventListener('touchend', e => {
  const dx = e.changedTouches[0].clientX - touchX;
  if (Math.abs(dx) > 50) goTo(dx < 0 ? current + 1 : current - 1);
});

window.addEventListener('hashchange', () => {
  if (ignoreHashChange) return;
  const target = indexFromHash(window.location.hash);
  if (target >= 0 && target !== current) {
    goTo(target, { behavior: 'smooth', shouldSyncHash: false });
  }
});

const initialFromHash = indexFromHash(window.location.hash);
if (initialFromHash >= 0) {
  goTo(initialFromHash, { behavior: 'auto', shouldSyncHash: false });
} else {
  setUI(0, true);
}
