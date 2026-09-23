const $ = selector => document.querySelector(selector);
const format = value => new Intl.NumberFormat('en', {maximumFractionDigits: 1}).format(value);
let result;

function render(horizon) {
  const rows = result.comparisons.filter(row => row.horizon_s === horizon);
  const prediction = rows[0].predicted_net;
  const net = rows.map(row => row.measured_switch_minus_keep);
  const crossing = rows.map(row => row.repayment_s).filter(value => value !== null);
  const min = Math.min(...net), max = Math.max(...net);
  const mean = key => rows.reduce((sum, row) => sum + row[key], 0) / rows.length;
  const notes = {
    20: 'WAIT tied immediate switching: both had zero qualified completions in this window. Waiting did not demonstrate avoided loss.',
    40: 'All three transition runs produced qualified service before declared warm-up completion. The old warm-up gate would have missed this gain.',
    120: 'The transition gained service over keeping A, but already running B delivered more. The advice did not improve on immediate switching.'
  };
  for (const button of document.querySelectorAll('[data-horizon]')) {
    button.disabled = false;
    button.setAttribute('aria-pressed', String(Number(button.dataset.horizon) === horizon));
  }
  $('#advice').textContent = rows[0].action === 'WAIT' ? 'Recorded advice: wait' : 'Recorded advice: switch';
  $('#summary').textContent = `At ${horizon} seconds, the frozen estimate was ${prediction} net qualified completions. Independent observations were ${net.join(', ')}.`;
  $('#prediction').textContent = format(prediction);
  $('#observed').textContent = min === max ? format(min) : `${format(min)}–${format(max)}`;
  $('#crossing').textContent = crossing.length ? `${Math.min(...crossing)}–${Math.max(...crossing)} s` : 'Not yet observed';
  $('#window-note').textContent = notes[horizon];
  $('#table-caption').textContent = `${horizon}-second window · all three independent seeds`;
  $('#seed-rows').replaceChildren(...rows.map(row => {
    const tr = document.createElement('tr');
    [row.seed, row.predicted_net, row.measured_switch_minus_keep, (row.error > 0 ? '+' : '') + row.error, format(row.declared_transition_complete_s) + ' s'].forEach((value, index) => {
      const cell = document.createElement(index ? 'td' : 'th');
      if (!index) cell.scope = 'row';
      cell.textContent = String(value);
      tr.append(cell);
    });
    return tr;
  }));
  const values = [
    ['Keep A', 0, 'keep'],
    ['Switch A → B', mean('measured_switch_minus_keep'), 'switch'],
    ['Already on B', mean('already_B_minus_keep'), 'reference']
  ];
  const scale = Math.max(...values.map(row => row[1]), 1);
  $('#bars').replaceChildren(...values.map(([label, value, kind]) => {
    const row = document.createElement('div'); row.className = 'bar-row';
    const name = document.createElement('span'); name.textContent = label;
    const track = document.createElement('div'); track.className = 'bar-track'; track.setAttribute('aria-hidden', 'true');
    const bar = document.createElement('div'); bar.className = `bar ${kind}`; bar.style.width = `${value / scale * 100}%`; track.append(bar);
    const count = document.createElement('strong'); count.textContent = format(value);
    row.append(name, track, count); return row;
  }));
  $('#case-result').setAttribute('aria-busy', 'false');
}

try {
  const response = await fetch('./result.json');
  if (!response.ok) throw Error('Recorded data unavailable');
  result = await response.json();
  if (result.valid !== 9 || result.all_offered !== 10800 || [20,40,120].some(h => result.comparisons.filter(r => r.horizon_s === h).length !== 3)) throw Error('Incomplete case data');
  render(40);
  document.querySelectorAll('[data-horizon]').forEach(button => button.addEventListener('click', () => render(Number(button.dataset.horizon))));
} catch {
  $('#load-error').hidden = false;
  $('#case-result').hidden = true;
}
