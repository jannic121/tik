// ── helpers ──────────────────────────────────────────────────────
const $ = id => document.getElementById(id);
const esc = s => s == null ? '' :
  String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');

function fmtBytes(n) {
  if (n == null) return '–';
  const u = ['B','KB','MB','GB','TB']; let i=0,v=n;
  while(v>=1024&&i<u.length-1){v/=1024;i++;} return v.toFixed(v<10&&i?1:0)+' '+u[i];
}
function fmtAgo(ts) {
  if (!ts) return 'never';
  const s = Math.floor(Date.now()/1000-ts);
  if (s<60) return s+'s ago'; if (s<3600) return Math.floor(s/60)+'m ago';
  if (s<86400) return Math.floor(s/3600)+'h ago'; return Math.floor(s/86400)+'d ago';
}
function fmtTime(ts) {
  if (!ts) return '–'; return new Date(ts*1000).toLocaleString();
}
function cp(id) { navigator.clipboard.writeText($(id).textContent); }

async function api(path, opts={}) {
  const r = await fetch(path, {
    credentials:'include',
    headers:{'Content-Type':'application/json',...(opts.headers||{})},
    ...opts
  });
  if (r.status===401) { window.location='/login'; return null; }
  return r;
}

// ── tab switching ─────────────────────────────────────────────────
function switchTab(name) {
  document.querySelectorAll('nav button').forEach(
    b=>b.classList.toggle('active', b.dataset.tab===name));
  document.querySelectorAll('.panel').forEach(
    p=>p.classList.toggle('active', p.id==='panel-'+name));
  ({servers:loadServers, watchers:loadWatchers, files:loadFiles,
    transcription:loadTranscription, updates:loadUpdates, catalog:loadCatalog,
    search:loadSearchIndex, chat:loadChat, routing:loadRouting,
    archive:loadArchive, deploy:updatePortSummary})[name]?.();
}

// Servers tab = recorders + storage/transcription together.
async function loadServers() { await Promise.all([loadBackends(), loadStorage()]); }

async function logout() {
  await api('/api/logout',{method:'POST'});
  window.location='/login';
}

async function reapNow(pk, btn) {
  if (btn) { btn.disabled = true; btn.textContent = 'Reaping…'; }
  try {
    const r = await api('/api/backends/'+pk+'/reap-now', {method:'POST'});
    if (r && r.ok) {
      const x = await r.json();
      alert(`Reaped: ${x.redundant||0} redundant, ${x.salvaged||0} salvaged, freed ${fmtBytes(x.freed_bytes)}`);
      loadCatalog();
    } else { alert('Reap failed'); if (btn){ btn.disabled=false; btn.textContent='Reap now'; } }
  } catch(e) { alert('Reap failed: '+e.message); if (btn){ btn.disabled=false; btn.textContent='Reap now'; } }
}

async function retryFailed(btn) {
  if (!confirm('Re-queue all failed transfers? Best done after the storage worker is updated.')) return;
  if (btn) { btn.disabled = true; btn.textContent = '…'; }
  try { const r = await api('/api/transfers/retry-failed', {method:'POST'});
    const x = (r&&r.ok) ? await r.json() : null;
    alert(x ? `Re-queued ${x.retried} transfer(s).` : 'Retry failed'); loadCatalog();
  } catch(e) { alert('Retry failed: '+e.message); }
}
async function clearFailed(btn) {
  if (!confirm('Delete all failed transfer records? Recordings still on a recorder are re-discovered automatically.')) return;
  if (btn) { btn.disabled = true; btn.textContent = '…'; }
  try { const r = await api('/api/transfers/clear-failed', {method:'POST'});
    const x = (r&&r.ok) ? await r.json() : null;
    alert(x ? `Cleared ${x.cleared} record(s).` : 'Clear failed'); loadCatalog();
  } catch(e) { alert('Clear failed: '+e.message); }
}

// ── Catalog (strangler shadow) ───────────────────────────────────
async function loadCatalog() {
  const el = $('catalog-content');
  el.innerHTML = '<div class="empty">Loading…</div>';
  const sr = await api('/api/catalog/stats');
  if (sr && sr.status===404) {
    el.innerHTML = '<div class="empty">Catalog shadow mode is off. Set '
      + '<code>CATALOG_ENABLED=1</code> in the control-plane env and restart.</div>';
    return;
  }
  const stats = (sr && sr.ok) ? await sr.json() : null;
  if (!stats) { el.innerHTML = '<div class="empty">Catalog not reachable.</div>'; return; }
  const dr = await api('/api/catalog/drift'); const drift = (dr&&dr.ok)? await dr.json() : null;
  const fr = await api('/api/flv-status');    const flv   = (fr&&fr.ok)? await fr.json() : null;
  const rr = await api('/api/catalog/readiness'); const ready = (rr&&rr.ok)? await rr.json() : null;
  const dk = await api('/api/disk-breakdown'); const disk = (dk&&dk.ok)? await dk.json() : null;
  const tx = await api('/api/transfers/summary'); const txs = (tx&&tx.ok)? await tx.json() : null;

  const bs = stats.by_state||{}, jobs = stats.jobs||[];
  const backlog = jobs.filter(j=>j.kind==='transcribe'&&j.state==='ready')
                      .reduce((a,j)=>a+j.count,0);
  const card=(label,val,sub)=>`<div style="background:#0d0d0d;border:1px solid #222;border-radius:8px;padding:12px 14px;min-width:120px;">
    <div style="font-size:11px;color:#888;text-transform:uppercase;letter-spacing:.05em;">${label}</div>
    <div style="font-size:22px;font-weight:600;margin-top:4px;">${val}</div>
    ${sub?`<div style="font-size:11px;color:#666;margin-top:2px;">${esc(sub)}</div>`:''}</div>`;

  let html = '';
  if (ready) {
    const badge=(ok,label)=>`<span style="display:inline-block;padding:3px 10px;border-radius:12px;font-size:12px;margin-right:8px;background:${ok?'#16351f':'#3a2a16'};color:${ok?'#7fe0a0':'#e0b070'};border:1px solid ${ok?'#2a5a3a':'#5a4020'};">${ok?'✓':'⏳'} ${label}</span>`;
    html += `<div style="margin-bottom:16px;">${badge(ready.transcription_cutover_ready,'transcription cutover ready')}${badge(ready.eviction_ready,'eviction ready')}`
      + ((ready.blocking&&ready.blocking.length)
         ? `<span style="font-size:12px;color:#888;">blocking: ${ready.blocking.map(esc).join(', ')}</span>`
         : `<span style="font-size:12px;color:#666;">catalog matches reality — safe to flip when you are</span>`)
      + `</div>`;
  }
  html += `<div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:16px;">
    ${card('Recordings',(stats.recordings||0).toLocaleString(),fmtBytes(stats.bytes))}
    ${card('On hot storage',(bs.stored||0).toLocaleString(),'state: stored')}
    ${card('Evicted',(bs.evicted||0).toLocaleString(),'cleaned off hot disk')}
    ${card('Transcribed',(stats.transcripts_done||0).toLocaleString(),'')}
    ${card('Transcribe backlog',backlog.toLocaleString(),'genuinely pending')}
  </div>`;

  const stateRows = Object.entries(bs).sort((a,b)=>b[1]-a[1]).map(([k,v]) =>
    `<span style="display:inline-block;margin:0 14px 6px 0;"><b>${v.toLocaleString()}</b> <span style="color:#888;">${esc(k)}</span></span>`).join('');
  if (stateRows) html += `<div style="margin-bottom:16px;font-size:13px;">${stateRows}</div>`;

  const cold = stats.cold||{};
  html += `<h3 style="font-size:13px;color:#aaa;margin:16px 0 8px;">Cold tier (cloud archive)</h3>
    <div style="font-size:13px;color:#ccc;">
      <span style="margin-right:18px;">archived: <b>${(cold.archived||0).toLocaleString()}</b></span>
      <span style="margin-right:18px;">safe to evict: <b>${(cold.evict_candidates||0).toLocaleString()}</b></span>
      <span>reclaimable: <b>${fmtBytes(cold.reclaimable_bytes)}</b></span></div>
    <div style="font-size:11px;color:#666;margin-top:4px;">verified cloud copy + local copy, &gt;7d old — safe to drop from hot disk (shadow: nothing is evicted)</div>`;

  if (disk && disk.length) {
    html += `<h3 style="font-size:13px;color:#aaa;margin:18px 0 8px;">Disk usage</h3>`;
    const ORDER=[['final_mp4','recordings'],['orphan_flv','orphan flv'],['redundant_flv','redundant flv ⚠'],['temp','temp ⚠'],['logs','logs'],['chat','chat'],['transcripts','transcripts'],['other','other']];
    disk.forEach(d=>{
      if (d.not_deployed){ html+=`<div style="font-size:12px;color:#888;margin-bottom:6px;">${esc(d.backend_id)}: disk breakdown not deployed — push update</div>`; return; }
      if (!d.reachable){ html+=`<div style="font-size:12px;color:#a55;margin-bottom:6px;">${esc(d.backend_id)}: unreachable</div>`; return; }
      const c=d.categories||{}, pct=d.disk_total?Math.round(d.disk_used/d.disk_total*100):0;
      const pc=pct>=90?'#e0607a':pct>=75?'#e0b070':'#888';
      html+=`<div style="background:#0d0d0d;border:1px solid #222;border-radius:8px;padding:12px 14px;margin-bottom:10px;">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
          <strong>${esc(d.backend_id)}</strong>
          <span style="font-size:12px;color:${pc};">${fmtBytes(d.disk_used)} / ${fmtBytes(d.disk_total)} (${pct}%)</span></div>
        <div style="font-size:12px;color:#ccc;display:flex;flex-wrap:wrap;gap:4px 16px;">
          ${ORDER.filter(([k])=>c[k]).map(([k,l])=>`<span>${l}: <b>${fmtBytes(c[k])}</b></span>`).join('')}
          <span style="color:#888;">outside recordings: <b>${fmtBytes(d.outside_recordings_bytes)}</b></span></div>
        ${d.reclaimable_bytes?`<div style="margin-top:8px;display:flex;align-items:center;gap:10px;">
          <span style="color:#e0b070;font-size:12px;">↻ ${fmtBytes(d.reclaimable_bytes)} reclaimable (redundant flv + temp)</span>
          <button class="ghost" style="font-size:11px;padding:3px 10px;" onclick="reapNow('${d.backend_pk}',this)">Reap now</button></div>`:''}
      </div>`;
    });
  }

  if (txs) {
    const failed=txs.failed||0, done=txs.done||0, inflight=(txs.pending||0)+(txs.transferring||0);
    html += `<h3 style="font-size:13px;color:#aaa;margin:18px 0 8px;">Transfers (recorder → storage)</h3>
      <div style="font-size:13px;color:#ccc;">
        <span style="margin-right:16px;">done: <b>${done.toLocaleString()}</b></span>
        <span style="margin-right:16px;">in-flight: <b>${inflight.toLocaleString()}</b></span>
        <span style="margin-right:12px;">failed: <b style="color:${failed?'#e0607a':'#ccc'}">${failed.toLocaleString()}</b></span>
        ${failed?`<button class="ghost" style="font-size:11px;padding:3px 10px;" onclick="retryFailed(this)">Retry failed</button>
          <button class="ghost" style="font-size:11px;padding:3px 10px;margin-left:6px;" onclick="clearFailed(this)">Clear failed</button>`:''}
      </div>`;
  }

  if (drift && drift.parity) {
    const c = drift.parity.counts||{};
    html += `<h3 style="font-size:13px;color:#aaa;margin:16px 0 8px;">Drift vs live fleet</h3>
      <div style="font-size:13px;color:#ccc;">
        <span style="margin-right:18px;">untracked on disk: <b>${c.live_only||0}</b></span>
        <span style="margin-right:18px;">tracked but missing: <b>${c.catalog_missing||0}</b></span>
        <span>size mismatch: <b>${c.size_mismatch||0}</b></span></div>`;
  }
  if (drift && drift.transcribe_compare) {
    const c = drift.transcribe_compare;
    html += `<div style="font-size:12px;color:#888;margin-top:6px;">transcribe shadow vs live — `
      + `catalog-only: ${c.catalog_only||0} · already done on a worker: ${c.already_done_live||0}</div>`;
  }

  if (flv && flv.length) {
    html += `<h3 style="font-size:13px;color:#aaa;margin:20px 0 8px;">Orphaned-flv reaper</h3>
      <table><thead><tr><th>Recorder</th><th>Status</th><th>Redundant</th><th>Salvaged</th>
        <th>Corrupt</th><th>Freed</th><th>Last run</th></tr></thead><tbody>
      ${flv.map(f=>{
        if (f.not_deployed) return `<tr><td>${esc(f.backend_id)}</td><td colspan="6" style="color:#888;">reaper not deployed — push update</td></tr>`;
        if (!f.reachable)   return `<tr><td>${esc(f.backend_id)}</td><td colspan="6" style="color:#a55;">unreachable</td></tr>`;
        return `<tr><td>${esc(f.backend_id)}</td>
          <td>${f.enabled?'<span style="color:#5a5;">on</span>':'<span style="color:#888;">off</span>'}</td>
          <td>${f.redundant||0}</td><td>${f.salvaged||0}</td>
          <td>${f.corrupt?`<span style="color:#c83;">${f.corrupt}</span>`:0}</td>
          <td>${fmtBytes(f.freed_bytes)}</td><td>${fmtAgo(f.last_run)}</td></tr>`;
      }).join('')}</tbody></table>`;
  }

  el.innerHTML = html;
}

// ── Backends ─────────────────────────────────────────────────────
async function loadBackends() {
  const r = await api('/api/backends'); if(!r) return;
  const data = await r.json();
  const el = $('backends-content');
  if (!data.length) { el.innerHTML='<div class="empty">No backends yet. Click <strong>Add backend</strong> to register one.</div>'; return; }

  // Fetch storage servers to detect colocation (same hostname = same VPS)
  let storageHosts = new Set();
  try {
    const sr = await api('/api/storage');
    if (sr && sr.ok) {
      const sl = await sr.json();
      sl.forEach(s => { try { storageHosts.add(new URL(s.url).hostname); } catch(_){} });
    }
  } catch(_) {}

  el.innerHTML = `<table>
    <thead><tr><th>Backend ID</th><th>Region</th><th>URL</th><th>Status</th>
      <th>Load</th><th>Disk free</th><th>Rev</th><th>Checked</th><th></th></tr></thead>
    <tbody>${data.map(b=>{
      const h=b.health||{};
      const hcls=b.last_health_ok?'ok':(b.last_health_check?'bad':'unknown');
      const htxt=b.last_health_ok?'OK':(b.last_health_check?'OFFLINE':'UNKNOWN');
      const load=h.max_watchers!=null?`${h.active_watchers}/${h.max_watchers}`:'–';
      let bHost=''; try { bHost=new URL(b.url).hostname; } catch(_){}
      const colocBadge = bHost && storageHosts.has(bHost)
        ? `<span style="background:#1a2a3a;color:#9cf;font-size:10px;padding:1px 5px;border-radius:3px;margin-left:4px;" title="Same VPS also runs a storage/transcription server — this is one machine doing both roles">📦 colocated · ${esc(bHost)}</span>`
        : '';
      return `<tr>
        <td><strong>${esc(b.backend_id)}</strong>${colocBadge}</td>
        <td class="dim">${esc(b.region||'')}</td>
        <td class="mono dim">${esc(b.url)}</td>
        <td><span class="badge ${hcls}">${htxt}</span></td>
        <td>${load}</td>
        <td class="dim">${fmtBytes(h.disk_free_bytes)}</td>
        <td class="mono dim">${esc(h.recorder_git_rev||'')}</td>
        <td class="dim">${fmtAgo(b.last_health_check)}</td>
        <td style="display:flex;gap:4px;white-space:nowrap;">
          <button class="ghost" onclick="openCookies('${b.id}','${esc(b.backend_id)}')"
            style="font-size:11px;padding:3px 8px;" title="Manage TikTok cookies">🍪</button>
          <button class="ghost" onclick="openPurge('${b.id}','${esc(b.backend_id)}')"
            style="font-size:11px;padding:3px 8px;" title="Delete recorder copies already confirmed on storage">🧹 Clean up</button>
          <button class="danger" onclick="deleteBackend('${b.id}','${esc(b.backend_id)}')">Remove</button>
        </td>
      </tr>`;
    }).join('')}</tbody></table>`;
}

function openModal(id) { $(id).classList.add('show'); }
function closeModal(id) { $(id).classList.remove('show'); }

function openAddBackend(prefillUrl='', prefillToken='') {
  $('b-url').value = prefillUrl;
  $('b-token').value = prefillToken;
  if($('b-ssh-host')) $('b-ssh-host').value='';
  if($('b-ssh-port')) $('b-ssh-port').value='';
  $('b-err').textContent = '';
  openModal('modal-backend');
  $('b-url').focus();
}

async function submitBackend() {
  $('b-err').textContent='';
  const url=$('b-url').value.trim(), token=$('b-token').value.trim();
  if (!url||!token){$('b-err').textContent='URL and token required';return;}
  const sshHost=($('b-ssh-host')?.value||'').trim();
  const sshPort=parseInt($('b-ssh-port')?.value||'',10);
  const r=await api('/api/backends',{method:'POST',body:JSON.stringify(
    {url,auth_token:token,ssh_host:sshHost||null,ssh_port:sshPort||null})});
  if (!r) return;
  if (r.ok){closeModal('modal-backend');loadBackends();return;}
  const e=await r.json().catch(()=>({}));
  $('b-err').textContent=e.detail||`Error ${r.status}`;
}

async function deleteBackend(id,label) {
  if (!confirm(`Remove backend "${label}"?\nAll its watchers will be stopped.`)) return;
  const r=await api(`/api/backends/${id}`,{method:'DELETE'});
  if (r&&r.status===204) loadBackends();
}

// ── Watchers ─────────────────────────────────────────────────────
async function loadWatchers() {
  const r=await api('/api/watchers'); if(!r) return;
  const data=await r.json();
  const el=$('watchers-content');
  if (!data.length){el.innerHTML='<div class="empty">No watchers. Click <strong>Add watcher</strong> to start tracking a creator.</div>';return;}
  const erroredCount = data.filter(w => w.reachable && w.state === 'error').length;
  const banner = erroredCount > 0
    ? `<div style="background:#3a1f1f;border:1px solid #6d2a2a;border-radius:6px;padding:10px 14px;
         margin-bottom:12px;display:flex;align-items:center;justify-content:space-between;">
         <span style="color:#f99;font-size:13px;">${erroredCount} watcher${erroredCount>1?'s':''} in error state.</span>
         <button class="primary" onclick="restartErrored(this)" style="font-size:12px;padding:5px 12px;">Re-enable all</button>
       </div>`
    : '';
  el.innerHTML=banner+`<table>
    <thead><tr><th>Username</th><th>State</th><th>Backend</th><th>Interval</th>
      <th>Chat</th><th>Failures</th><th>Last error</th><th>Added</th><th></th></tr></thead>
    <tbody>${data.map(w=>{
      const st=w.reachable?(w.state||'idle'):'offline';
      const f=w.consecutive_failures||0;
      const reenable = (st==='error')
        ? `<button class="ghost" onclick="restartWatcher('${esc(w.username)}',this)"
             style="font-size:11px;padding:3px 8px;color:#6ee7a7;border-color:#2d5a3f;" title="Reset and restart">Re-enable</button> `
        : '';
      const chatOn = w.capture_chat !== false;
      const chatCell = w.reachable
        ? `<button class="ghost" onclick="toggleChat('${esc(w.username)}',${!chatOn},this)"
             style="font-size:11px;padding:3px 8px;${chatOn?'color:#6ee7a7;border-color:#2d5a3f':'color:#888;border-color:#3a3a3a'}"
             title="Click to ${chatOn?'disable':'enable'} chat capture">💬 ${chatOn?'on':'off'}</button>`
        : '<span class="dim">—</span>';
      return `<tr>
        <td><strong>${esc(w.username)}</strong></td>
        <td><span class="badge ${st}">${st}</span></td>
        <td class="dim">${esc(w.backend_label)}</td>
        <td class="dim">${w.automatic_interval_min}m</td>
        <td>${chatCell}</td>
        <td>${f>0?`<span style="color:#fa6">${f}</span>`:'0'}</td>
        <td class="dim" style="max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(w.last_error||'')}">${esc(w.last_error||'')}</td>
        <td class="dim">${fmtAgo(w.created_at)}</td>
        <td style="white-space:nowrap;">${reenable}<button class="danger" onclick="deleteWatcher('${esc(w.username)}')">Remove</button></td>
      </tr>`;
    }).join('')}</tbody></table>`;
}

async function toggleChat(username, enable, btn) {
  if (btn){ btn.disabled=true; btn.textContent='…'; }
  const r=await api(`/api/watchers/${encodeURIComponent(username)}/chat`,
    {method:'POST',body:JSON.stringify({enabled:enable})});
  if (r && r.ok){ loadWatchers(); }
  else { if(btn){btn.disabled=false;} alert('Chat toggle failed — backend may need a push-update.'); }
}

async function openChatHealth() {
  openModal('modal-chathealth');
  const box = $('chathealth-content');
  box.innerHTML = '<div class="dim" style="padding:10px;">Checking backends…</div>';
  const r = await api('/api/chat-diag');
  if (!r || !r.ok) { box.innerHTML = '<div class="dim">Could not load diagnostics.</div>'; return; }
  const diags = await r.json();
  if (!diags.length) { box.innerHTML = '<div class="empty">No backends registered.</div>'; return; }
  box.innerHTML = diags.map(d => {
    if (!d.reachable)
      return `<div class="card" style="border:1px solid #5a2d2d;border-radius:8px;padding:12px;margin-bottom:10px;">
        <strong>${esc(d.backend_id||d.backend_pk)}</strong> <span style="color:#e5534b;">unreachable</span></div>`;
    const okBadge = (v,label) => `<span style="color:${v?'#6ee7a7':'#e5534b'};font-size:12px;">${v?'✓':'✗'} ${label}</span>`;
    const libLine = d.tiktoklive_installed
      ? okBadge(true, `TikTokLive ${esc(d.tiktoklive_version||'')}`)
      : okBadge(false, 'TikTokLive NOT installed') + (d.tiktoklive_error?` <span class="dim">(${esc(d.tiktoklive_error)})</span>`:'');
    const fix = !d.tiktoklive_installed ? `
      <div style="background:#3a2a12;border:1px solid #5a4520;border-radius:6px;padding:10px;margin-top:8px;font-size:12px;">
        <strong>This is why chat isn't recording.</strong> TikTokLive isn't installed on this backend
        (push-update ships code but not dependencies). Install it now over SSH:
        <div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:8px;align-items:center;">
          <input id="cd-user-${d.backend_pk}" placeholder="ssh user" value="root" style="width:90px;"/>
          <input id="cd-port-${d.backend_pk}" type="number" value="22" style="width:64px;"/>
          <select id="cd-auth-${d.backend_pk}" onchange="$('cd-pw-${d.backend_pk}').style.display=this.value==='password'?'':'none';$('cd-key-${d.backend_pk}').style.display=this.value==='key'?'':'none';">
            <option value="key">key</option><option value="password">password</option></select>
          <input id="cd-key-${d.backend_pk}" placeholder="~/.ssh/id_rsa" style="width:130px;"/>
          <input id="cd-pw-${d.backend_pk}" type="password" placeholder="password" style="width:120px;display:none;"/>
          <button class="primary" onclick="installChatDeps('${d.backend_pk}')">Install TikTokLive</button>
        </div>
        <pre id="cd-out-${d.backend_pk}" style="display:none;background:#111;border-radius:5px;padding:8px;margin-top:8px;max-height:160px;overflow:auto;white-space:pre-wrap;"></pre>
      </div>` : '';
    const watchers = (d.watchers||[]);
    const wrows = watchers.length ? watchers.map(w => {
      const dot = !w.capture_chat ? '<span class="dim">off</span>'
                : w.running ? '<span style="color:#6ee7a7;">● running</span>'
                : '<span style="color:#e5534b;">● not running</span>';
      const exit = (w.last_exit!=null) ? ` <span class="dim">last exit ${w.last_exit}${w.last_exit===3?' = lib missing':''}</span>` : '';
      const logTail = (w.recent_log||[]).slice(-8).map(esc).join('\n');
      return `<div style="margin:8px 0;padding:8px;border:1px solid #262626;border-radius:6px;">
        <div style="font-size:13px;"><strong>${esc(w.username)}</strong> · ${dot}${exit}
          ${w.restarts?` <span class="dim">· ${w.restarts} restarts</span>`:''}
          ${w.uptime_sec!=null?` <span class="dim">· up ${Math.round(w.uptime_sec)}s</span>`:''}</div>
        ${logTail?`<pre style="background:#0e0e0e;border-radius:5px;padding:6px;margin:6px 0 0;font-size:11px;max-height:140px;overflow:auto;white-space:pre-wrap;color:#9fb4c8;">${logTail}</pre>`:'<div class="dim" style="font-size:11px;margin-top:4px;">no recorder output yet</div>'}
      </div>`;
    }).join('') : '<div class="dim" style="font-size:12px;">no watchers on this backend</div>';
    return `<div class="card" style="border:1px solid #2a2a2a;border-radius:8px;padding:12px;margin-bottom:12px;">
      <div style="display:flex;justify-content:space-between;align-items:baseline;">
        <strong>${esc(d.backend_id||d.backend_pk)}</strong>
        <span style="font-size:12px;">${libLine}</span>
      </div>
      <div style="font-size:12px;margin-top:4px;">
        ${okBadge(d.chat_recorder_present,'chat_recorder.py present')} ·
        ${okBadge(d.sign_api_key_set,'sign API key set')}
        ${!d.sign_api_key_set?'<span class="dim">(free tier; fine for a few creators, set SIGN_API_KEY for many)</span>':''}
      </div>
      ${fix}
      <div style="margin-top:10px;">${wrows}</div>
    </div>`;
  }).join('');
}

async function installChatDeps(pk) {
  const out = $('cd-out-'+pk); out.style.display=''; out.textContent='';
  const body = { ssh_user:$('cd-user-'+pk).value.trim(), ssh_port:parseInt($('cd-port-'+pk).value,10),
    auth_method:$('cd-auth-'+pk).value, ssh_password:$('cd-pw-'+pk).value, ssh_key_path:$('cd-key-'+pk).value.trim() };
  try {
    const resp = await fetch(`/api/backends/${pk}/install-chat-deps`, {method:'POST',credentials:'include',
      headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)});
    if (!resp.ok) { out.textContent += `[ERROR] HTTP ${resp.status}\n`+await resp.text(); return; }
    const reader=resp.body.getReader(), dec=new TextDecoder();
    while(true){ const {value,done}=await reader.read(); if(done)break;
      out.textContent += dec.decode(value,{stream:true}); out.scrollTop=out.scrollHeight; }
  } catch(e){ out.textContent += '\n[ERROR] '+e.message; }
}

async function restartWatcher(username, btn) {
  if (btn){ btn.disabled=true; btn.textContent='…'; }
  const r=await api('/api/watchers/'+encodeURIComponent(username)+'/restart',{method:'POST'});
  if (r && r.ok) { loadWatchers(); }
  else if (btn){ btn.disabled=false; btn.textContent='Re-enable'; alert('Re-enable failed — backend may be unreachable.'); }
}

async function restartErrored(btn) {
  if (btn){ btn.disabled=true; btn.textContent='Re-enabling…'; }
  const r=await api('/api/watchers/restart-errored',{method:'POST'});
  if (r && r.ok) { const d=await r.json(); loadWatchers();
    if (d.failed && d.failed.length) alert(`Re-enabled ${d.count}. Failed: ${d.failed.join(', ')}`); }
  else if (btn){ btn.disabled=false; btn.textContent='Re-enable all'; }
}

async function openAddWatcher() {
  $('w-user').value=''; $('w-int').value='3'; $('w-err').textContent=''; $('w-chat').checked=true;
  const r=await api('/api/backends'); if(!r) return;
  const bs=await r.json();
  $('w-back').innerHTML='<option value="">Auto (least-loaded)</option>'+
    bs.filter(b=>b.last_health_ok)
      .map(b=>`<option value="${b.id}">${esc(b.backend_id)}</option>`).join('');
  openModal('modal-watcher'); $('w-user').focus();
}

async function submitWatcher() {
  $('w-err').textContent='';
  const username=$('w-user').value.trim();
  if (!username){$('w-err').textContent='Username required';return;}
  const r=await api('/api/watchers',{method:'POST',body:JSON.stringify({
    username, automatic_interval_min:parseInt($('w-int').value,10),
    backend_pk:$('w-back').value||null,
    capture_chat:$('w-chat').checked
  })});
  if (!r) return;
  if (r.ok){closeModal('modal-watcher');loadWatchers();return;}
  const e=await r.json().catch(()=>({}));
  $('w-err').textContent=e.detail||`Error ${r.status}`;
}

async function deleteWatcher(username) {
  if (!confirm(`Remove watcher for ${username}?`)) return;
  const r=await api(`/api/watchers/${encodeURIComponent(username)}`,{method:'DELETE'});
  if (r&&r.status===204) loadWatchers();
}

// ── Files ─────────────────────────────────────────────────────────
let _filesCache=null, _highlightFile=null;

async function loadFiles() {
  const r=await api('/api/files'); if(!r) return;
  const data=await r.json();

  // Fetch transcript + transfer statuses + live storage locations + chat matches
  let ts={}, xfr={}, prog={}, locs={}, chatm={};
  if (data.length) try {
    const [tr,tx,tp,tl,cm]=await Promise.all([
      api('/api/transcript-statuses'),
      api('/api/transfer-statuses'),
      api('/api/transfer-progress'),
      api('/api/file-locations'),
      api('/api/chat-matches'),
    ]);
    if(tr&&tr.ok) ts=await tr.json();
    if(tx&&tx.ok) xfr=await tx.json();
    if(tp&&tp.ok) prog=await tp.json();
    if(tl&&tl.ok) locs=await tl.json();
    if(cm&&cm.ok) chatm=await cm.json();
  } catch(_){}

  _filesCache={data,ts,xfr,prog,locs,chatm};
  renderFiles();
}

// Jump to a file from another tab (e.g. transcript search) and flash its row.
function gotoFile(filename){
  const fn=(filename||'').split('/').pop();
  _highlightFile=fn;
  const fi=$('files-filter'); if(fi) fi.value=fn;
  switchTab('files');   // triggers loadFiles(), which honours the filter + highlight
}

function renderFiles(){
  if(!_filesCache) return;
  const {data,ts,xfr,prog,locs,chatm={}}=_filesCache;
  const el=$('files-content');
  if (!data.length){el.innerHTML='<div class="empty">No files. Recordings appear here as creators go live.</div>';return;}
  const q=($('files-filter')?.value||'').trim().toLowerCase();
  const items = q ? data.filter(f=>((f.path||'').split('/').pop()+' '+(f.username||'')).toLowerCase().includes(q)) : data;
  if(!items.length){el.innerHTML=`<div class="empty">No files match <strong>${esc(q)}</strong>.</div>`;return;}

  el.innerHTML=`<table>
    <thead><tr><th>Username</th><th>Backend</th><th>File</th><th>Size</th><th>Modified</th><th>Storage</th><th>Transcript</th><th></th></tr></thead>
    <tbody>${items.map(f=>{
      const fname=f.path.split('/').pop();
      const dlUrl = f.backend_pk
        ? `/api/files/download?backend_pk=${encodeURIComponent(f.backend_pk)}&path=${encodeURIComponent(f.path)}`
        : `/api/files/download?storage_sid=${encodeURIComponent(f.storage_sid)}&path=${encodeURIComponent(f.path)}`;

      // Transfer status cell
      const xs=xfr[fname]||'none';
      const here=(locs[fname]||[]).map(l=>l.label);
      let xCell='';
      if(here.length){
        // Ground truth: the file is physically on these storage server(s).
        // Reflects moves immediately; lists both if mid-move (source+dest).
        const label = here.map(esc).join(', ');
        xCell=`<span style="background:#1a2e1a;color:#6ee7a7;font-size:11px;padding:2px 8px;border-radius:3px;" title="Current storage location">✓ on ${label}</span>`;
      } else if(xs==='none'){
        xCell=`<button onclick="queueTransfer('${f.backend_pk}',${JSON.stringify(f.path)})"
          style="background:#2a2a3a;color:#9cf;border:1px solid #3a4a7a;padding:3px 8px;
                 border-radius:3px;font-size:11px;cursor:pointer;">↑ Upload</button>`;
      } else if(xs==='pending'){
        xCell='<span style="background:#2a2a2a;color:#888;font-size:11px;padding:2px 8px;border-radius:3px;">⏳ Queued</span>';
      } else if(xs==='transferring'){
        const p=prog[fname]; const pct=p?p.pct:0; const done=p?p.bytes_done:0; const tot=p?p.size_bytes:0;
        const bar=pct>0
          ? `<div style="margin-top:3px;background:#2a2a2a;border-radius:3px;height:4px;width:110px;overflow:hidden;display:inline-block;vertical-align:middle;margin-left:6px;">
               <div style="background:#fc6;height:100%;width:${pct}%;transition:width .5s;"></div></div>
             <span style="font-size:10px;color:#888;margin-left:4px;">${pct}% &middot; ${fmtBytes(done)}/${fmtBytes(tot)}</span>`
          : '';
        xCell=`<div><span style="background:#3a3a1a;color:#fc6;font-size:11px;padding:2px 8px;border-radius:3px;">↑ Uploading</span>${bar}</div>`;
      } else if(xs==='done'){
        xCell='<span style="background:#1a2e1a;color:#6ee7a7;font-size:11px;padding:2px 8px;border-radius:3px;">✓ On storage</span>';
      } else if(xs==='failed'){
        xCell=`<span style="background:#4a1f1f;color:#f99;font-size:11px;padding:2px 8px;border-radius:3px;" title="Click to retry">✕ Failed</span>
               <button onclick="queueTransfer('${f.backend_pk}',${JSON.stringify(f.path)})"
                 style="background:#3a1a1a;color:#f99;border:1px solid #6d2a2a;padding:3px 6px;
                        border-radius:3px;font-size:11px;cursor:pointer;margin-left:4px;">Retry</button>`;
      }

      // Transcript status cell
      const tst=ts[fname]||'none';
      let tsCell='';
      if(tst==='none'){
        tsCell='<span style="color:#555;font-size:12px;">No transcript</span>';
      } else if(tst==='processing'){
        tsCell='<span style="background:#3a3a1a;color:#fc6;font-size:11px;padding:2px 8px;border-radius:3px;">⏳ Transcribing…</span>';
      } else if(tst==='pending'){
        tsCell='<span style="background:#2a2a2a;color:#888;font-size:11px;padding:2px 8px;border-radius:3px;">⏳ Pending</span>';
      } else if(tst==='done'){
        tsCell=`<span style="background:#1a2e1a;color:#6ee7a7;font-size:11px;padding:2px 8px;border-radius:3px;">✓ Available</span>
                <button onclick="viewTranscript(${JSON.stringify(fname)})"
                  style="background:#1f3a2a;color:#6ee7a7;border:1px solid #2d5a3f;padding:3px 8px;
                         border-radius:3px;font-size:11px;cursor:pointer;margin-left:4px;">View</button>
                <a href="/api/transcript-download?filename=${encodeURIComponent(fname)}"
                   download="${esc(fname.replace('.mp4','_transcript.txt'))}"
                   style="background:#1a2a3a;color:#9cf;border:1px solid #2a4a6a;padding:3px 8px;
                          border-radius:3px;font-size:11px;text-decoration:none;margin-left:2px;">↓ .txt</a>`;
      }

      return `<tr data-fname="${esc(fname)}">
        <td><strong>${esc(f.username)}</strong></td>
        <td class="dim">${f.backend_pk ? esc(f.backend_label) : '<span style="color:#6ee7a7;font-size:11px;">on storage</span>'}</td>
        <td class="mono dim" style="font-size:11px;">${esc(fname)}</td>
        <td>${fmtBytes(f.size_bytes)}</td>
        <td class="dim">${fmtTime(f.mtime)}</td>
        <td style="white-space:nowrap;">${xCell}</td>
        <td style="white-space:nowrap;">${tsCell}</td>
        <td style="display:flex;gap:4px;align-items:center;white-space:nowrap;">
          <a href="${dlUrl}" download="${esc(fname)}"
             style="background:#2a3a4a;color:#9cf;border:1px solid #3a5a7a;padding:4px 8px;
                    border-radius:4px;font-size:12px;text-decoration:none;">↓ MP4</a>${
          chatm[fname] ? `<button onclick='openChatLog(${JSON.stringify(chatm[fname])},${JSON.stringify(f.username)})' title="open the matched chat log"
             style="background:#2a1f3a;color:#c9f;border:1px solid #4a3a6a;padding:4px 8px;border-radius:4px;font-size:12px;cursor:pointer;">💬 chat</button>` : ''}
          <button class="danger" onclick="deleteFile(${f.backend_pk?`'${f.backend_pk}'`:'null'},${JSON.stringify(f.path)},${f.storage_sid?`'${f.storage_sid}'`:'null'})">✕</button>
        </td>
      </tr>`;}).join('')}</tbody></table>`;

  if(_highlightFile){
    const want=_highlightFile; _highlightFile=null;
    el.querySelectorAll('tr[data-fname]').forEach(tr=>{
      if(tr.getAttribute('data-fname')===want){
        tr.scrollIntoView({block:'center'});
        tr.style.transition='background .4s'; tr.style.background='#2d3a4a';
        setTimeout(()=>{tr.style.background='';},1600);
      }
    });
  }
}


// ── Transcription monitoring ─────────────────────────────────────
function _fmtDur(sec){
  if(sec==null) return '—';
  sec=Math.round(sec);
  const h=Math.floor(sec/3600), m=Math.floor((sec%3600)/60), ss=sec%60;
  if(h) return h+'h '+m+'m';
  if(m) return m+'m '+String(ss).padStart(2,'0')+'s';
  return ss+'s';
}
function _bar(pct, color){
  pct=Math.max(0,Math.min(100,pct||0));
  return `<div style="background:#2a2a2a;border-radius:3px;height:8px;width:100%;overflow:hidden;">
    <div style="background:${color};height:100%;width:${pct}%;transition:width .6s;"></div></div>`;
}
function _usageColor(p){ return p>=90?'#f87171':p>=70?'#fbbf24':'#6ee7a7'; }

async function loadTranscription(){
  const r=await api('/api/transcription-overview'); if(!r) return;
  const servers=await r.json();
  const el=$('transcription-content');
  if(!servers.length){ el.innerHTML='<div class="empty">No storage/transcription servers registered.</div>'; return; }

  el.innerHTML = servers.map(sv=>{
    if(!sv.reachable || !sv.detail){
      return `<div class="card" style="border-left:3px solid #f87171;margin-bottom:14px;">
        <div style="display:flex;justify-content:space-between;align-items:center;">
          <strong>${esc(sv.label)}</strong>
          <span style="color:#f87171;font-size:12px;">● unreachable</span></div></div>`;
    }
    const d=sv.detail, sys=d.system||{}, tp=d.throughput||{};
    const active=d.active||[];
    const transOff = d.transcribe_enabled===false;

    // status pill
    let pill;
    if(transOff) pill='<span style="background:#33271a;color:#fbbf24;font-size:12px;padding:2px 10px;border-radius:10px;">⏸ storage-only</span>';
    else if(active.length) pill=`<span style="background:#1a2e1a;color:#6ee7a7;font-size:12px;padding:2px 10px;border-radius:10px;">▶ transcribing ${active.length}</span>`;
    else pill='<span style="background:#222;color:#888;font-size:12px;padding:2px 10px;border-radius:10px;">○ idle</span>';

    // system metrics
    const cpu=sys.cpu_percent, mem=sys.mem_percent, load=sys.loadavg, cores=sys.cpu_count;
    const diskFree=d.disk_free_bytes, diskTot=d.disk_total_bytes;
    const diskPct = (diskFree!=null&&diskTot)? Math.round(100*(1-diskFree/diskTot)):null;

    const metric=(label,val,pct,extra)=>`
      <div style="flex:1;min-width:120px;">
        <div style="font-size:11px;color:#888;display:flex;justify-content:space-between;">
          <span>${label}</span><span>${val}</span></div>
        ${pct!=null?_bar(pct,_usageColor(pct)):''}
        ${extra?`<div style="font-size:10px;color:#666;margin-top:2px;">${extra}</div>`:''}
      </div>`;

    const loadStr = load? load.join('  ') + (cores?`  / ${cores} cores`:'') : '—';
    const sysRow=`<div style="display:flex;gap:14px;flex-wrap:wrap;margin:12px 0;">
      ${metric('CPU', cpu!=null?cpu+'%':'—', cpu)}
      ${metric('Memory', mem!=null?mem+'%':'—', mem,
               (sys.mem_available!=null&&sys.mem_total)?`${fmtBytes(sys.mem_available)} free of ${fmtBytes(sys.mem_total)}`:'')}
      ${metric('Disk', diskPct!=null?diskPct+'%':'—', diskPct,
               diskFree!=null?`${fmtBytes(diskFree)} free`:'')}
      <div style="flex:1;min-width:120px;">
        <div style="font-size:11px;color:#888;">Load avg (1·5·15m)</div>
        <div style="font-size:13px;color:#ccc;margin-top:3px;" class="mono">${loadStr}</div>
      </div>
    </div>`;

    // headline stats
    const stat=(v,l,c)=>`<div style="text-align:center;padding:0 14px;">
      <div style="font-size:20px;font-weight:600;color:${c||'#eee'};">${v}</div>
      <div style="font-size:10px;color:#888;text-transform:uppercase;">${l}</div></div>`;
    const statsRow=`<div style="display:flex;gap:6px;flex-wrap:wrap;background:#1c1c1c;border-radius:6px;padding:10px 0;margin-bottom:10px;">
      ${stat(d.queue_depth??0,'Queued', (d.queue_depth>20)?'#fbbf24':'#eee')}
      ${stat(active.length,'Active','#6ee7a7')}
      ${stat(tp.completed_last_hour??0,'Done / hr')}
      ${stat(tp.avg_speed!=null?tp.avg_speed+'×':'—','Speed (realtime)', (tp.avg_speed!=null&&tp.avg_speed<1)?'#fbbf24':'#6ee7a7')}
      ${stat(d.gave_up??0,'Gave up', (d.gave_up>0)?'#f87171':'#eee')}
    </div>`;

    // active files with live progress
    let activeHtml='';
    if(active.length){
      activeHtml = active.map(a=>{
        const spd = (a.processed_sec&&a.elapsed)? (a.processed_sec/a.elapsed):null;
        const eta = (a.duration&&a.processed_sec&&spd&&spd>0)? (a.duration-a.processed_sec)/spd : null;
        const pct = a.pct||0;
        return `<div style="margin:8px 0;">
          <div style="display:flex;justify-content:space-between;font-size:12px;">
            <span class="mono" style="color:#ccc;">${esc(a.filename)}${a.stalled?' <span style="color:#f87171;">⚠ stalled</span>':''}</span>
            <span style="color:#888;">${Math.round(pct)}% · ${_fmtDur(a.elapsed)} elapsed${spd?` · ${spd.toFixed(1)}×`:''}${eta!=null?` · ETA ${_fmtDur(eta)}`:''}</span>
          </div>
          ${_bar(pct, a.stalled?'#f87171':'#fc6')}
          <div style="font-size:10px;color:#666;margin-top:2px;">
            ${a.processed_sec?_fmtDur(a.processed_sec):'0s'}${a.duration?` of ${_fmtDur(a.duration)} audio`:''}</div>
        </div>`;
      }).join('');
      activeHtml=`<div style="margin-bottom:10px;"><div style="font-size:11px;color:#888;text-transform:uppercase;margin-bottom:4px;">Now transcribing</div>${activeHtml}</div>`;
    } else if(!transOff){
      activeHtml='<div style="color:#666;font-size:12px;margin-bottom:10px;">Nothing transcribing right now.</div>';
    }

    // recent completed
    const rc=(d.recent_completed||[]).slice(0,8);
    let rcHtml='';
    if(rc.length){
      rcHtml='<div style="font-size:11px;color:#888;text-transform:uppercase;margin:6px 0 4px;">Recently completed</div>'+
        rc.map(c=>{
          const spd=(c.duration&&c.elapsed_sec)?(c.duration/c.elapsed_sec):null;
          const name=(c.mp4||'').split('/').pop();
          return `<div style="display:flex;justify-content:space-between;font-size:11px;color:#aaa;padding:2px 0;">
            <span class="mono">${esc(name)}</span>
            <span>${c.duration?_fmtDur(c.duration)+' audio':''} · ${_fmtDur(c.elapsed_sec)}${spd?` · ${spd.toFixed(1)}×`:''}${c.language?` · ${esc(c.language)}`:''}${c.words?` · ${c.words}w`:''}</span>
          </div>`;
        }).join('');
    }
    // recent failed
    const rf=(d.recent_failed||[]).slice(0,5);
    let rfHtml='';
    if(rf.length){
      rfHtml='<div style="font-size:11px;color:#f87171;text-transform:uppercase;margin:6px 0 4px;">Recent failures</div>'+
        rf.map(f=>{
          const name=(f.mp4||'').split('/').pop();
          const tag = f.permanent ? '<span style="color:#f87171;">gave up</span>'
                    : `<span style="color:#fbbf24;">retrying${f.attempts?` (${f.attempts}/${d.max_attempts||3})`:''}</span>`;
          const detail = f.detail ? ` title="${esc((f.detail||'').slice(-400))}"` : '';
          return `<div style="font-size:11px;color:#d99;padding:2px 0;"${detail}>
            <div style="display:flex;justify-content:space-between;">
              <span class="mono">${esc(name)}</span><span>${tag}</span></div>
            <div style="color:#a77;">${esc(f.error||'')}</div></div>`;
        }).join('');
      if(d.gave_up>0){
        rfHtml+=`<button class="ghost" style="margin-top:6px;font-size:11px;"
          onclick="retryFailed('${sv.sid}',this)">↻ Retry ${d.gave_up} given-up file${d.gave_up>1?'s':''}</button>`;
      }
    }

    const cfg=`${esc(d.model||'?')} · ${d.concurrency??'?'}× · VAD ${d.vad===false?'off':'on'} · beam ${d.beam_size??'?'}`;
    return `<div class="card" style="margin-bottom:14px;">
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <div><strong>${esc(sv.label)}</strong>
          <span style="font-size:11px;color:#666;margin-left:8px;" class="mono">${esc(d.build||'')}</span></div>
        ${pill}
      </div>
      <div style="font-size:12px;color:#888;margin-top:2px;">${cfg}</div>
      ${transOff?'<div style="color:#fbbf24;font-size:12px;margin-top:6px;">This server stores &amp; serves files but is not transcribing. Files queue elsewhere.</div>':''}
      ${sysRow}
      ${statsRow}
      ${activeHtml}
      ${rcHtml}
      ${rfHtml}
      <div style="margin-top:8px;display:flex;gap:8px;align-items:center;flex-wrap:wrap;">
        <button class="ghost" style="font-size:11px;" onclick="oomProtect('${sv.sid}',this)"
          title="Make transcription the OOM victim so the control plane survives memory pressure">🛡 Protect from OOM</button>
        <pre id="oom-${sv.sid}" style="display:none;flex:1;background:#111;border-radius:6px;padding:8px;font-size:11px;white-space:pre-wrap;margin:0;"></pre>
      </div>
    </div>`;
  }).join('');
}

async function oomProtect(sid, btn){
  const out=$('oom-'+sid);
  btn.disabled=true; btn.textContent='Applying…';
  out.style.display='block'; out.textContent='';
  try{
    const res=await fetch('/api/storage/'+sid+'/oom-protect',{method:'POST',credentials:'include'});
    const reader=res.body.getReader(), dec=new TextDecoder();
    while(true){ const {done,value}=await reader.read(); if(done) break;
      out.textContent+=dec.decode(value); out.scrollTop=out.scrollHeight; }
  }catch(e){ out.textContent+='\n[error] '+e; }
  btn.disabled=false; btn.textContent='🛡 Protect from OOM';
}


// ── Control-plane self-update ────────────────────────────────────
async function runSelfUpdate(){
  if(!confirm('Update the control plane from /root/tt-recorder.zip and restart it now?')) return;
  const btn=$('selfupdate-btn'), out=$('selfupdate-out');
  btn.disabled=true; btn.textContent='Updating…';
  out.style.display='block'; out.textContent='';
  let restarting=false;
  try{
    const res=await fetch('/api/self-update',{method:'POST',credentials:'include'});
    const reader=res.body.getReader(), dec=new TextDecoder();
    while(true){
      const {done,value}=await reader.read(); if(done) break;
      const chunk=dec.decode(value); out.textContent+=chunk; out.scrollTop=out.scrollHeight;
      if(chunk.includes('Restarting')) restarting=true;
    }
  }catch(e){ out.textContent+='\n(connection closed by restart)'; restarting=true; }
  if(restarting){
    out.textContent+='\nWaiting for the control plane to come back…\n';
    for(let i=0;i<40;i++){
      await new Promise(r=>setTimeout(r,1500));
      try{
        const r=await fetch('/login',{cache:'no-store'});
        if(r.ok){ out.textContent+='Back up — reloading.\n'; setTimeout(()=>location.reload(),700); return; }
      }catch(_){}
    }
    out.textContent+='Still waiting — reload the page manually in a moment.\n';
    btn.disabled=false; btn.textContent='⤓ Update from /root/tt-recorder.zip';
  } else {
    btn.disabled=false; btn.textContent='⤓ Update from /root/tt-recorder.zip';
  }
}




// ── Install control plane as a systemd service ───────────────────
async function installCpService(){
  if(!confirm('Install/refresh the control-plane systemd service now? If it is running manually, it will switch over (the dashboard reconnects automatically).')) return;
  const btn=$('cpservice-btn'), out=$('cpservice-out');
  btn.disabled=true; btn.textContent='Installing…';
  out.style.display='block'; out.textContent='';
  let switching=false;
  try{
    const res=await fetch('/api/control-plane/install-service',{method:'POST',credentials:'include'});
    const reader=res.body.getReader(), dec=new TextDecoder();
    while(true){
      const {done,value}=await reader.read(); if(done) break;
      const chunk=dec.decode(value); out.textContent+=chunk; out.scrollTop=out.scrollHeight;
      if(chunk.includes('Switching')||chunk.includes('reloading')) switching=true;
    }
  }catch(e){ out.textContent+='\n(connection closed — switching to systemd)'; switching=true; }
  if(switching){
    out.textContent+='\nWaiting for the service to come up…\n';
    for(let i=0;i<40;i++){
      await new Promise(r=>setTimeout(r,1500));
      try{ const r=await fetch('/login',{cache:'no-store'});
        if(r.ok){ out.textContent+='Up — reloading.\n'; setTimeout(()=>location.reload(),700); return; } }catch(_){}
    }
    out.textContent+='Still waiting — reload manually in a moment.\n';
  }
  btn.disabled=false; btn.textContent='🛡 Install control plane service';
}

async function retryFailed(sid, btn){
  if(btn){ btn.disabled=true; btn.textContent='Retrying…'; }
  const r=await api('/api/storage/'+sid+'/retry-failed',{method:'POST'});
  if(r&&r.ok){ setTimeout(loadTranscription, 1000); }
  else if(btn){ btn.disabled=false; btn.textContent='↻ Retry failed'; }
}

// ── Updates (one-click, saved-credential updates) ────────────────
async function loadUpdates(){
  const r=await api('/api/update-status'); if(!r) return;
  const d=await r.json();
  const hosts=d.hosts||[];
  const sum=$('updates-summary'), el=$('updates-content');

  // summary banner
  const outdated=d.count_outdated||0, ready=d.count_ready||0;
  if(!hosts.length){
    sum.innerHTML=''; el.innerHTML='<div class="empty">No machines registered yet.</div>'; return;
  }
  if(outdated===0){
    sum.innerHTML=`<div style="background:#16241b;border:1px solid #2c5;border-radius:8px;padding:12px 16px;display:flex;justify-content:space-between;align-items:center;">
      <span style="color:#6ee7a7;font-weight:600;">✓ All ${hosts.length} machine${hosts.length>1?'s':''} up to date</span>
      <span class="dim mono" style="font-size:11px;">control plane · ${esc(d.expected||'')}</span></div>`;
  } else {
    sum.innerHTML=`<div style="background:#241f16;border:1px solid #b8860b;border-radius:8px;padding:12px 16px;display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;">
      <span style="color:#fbbf24;font-weight:600;">⬆ ${outdated} of ${hosts.length} machine${hosts.length>1?'s':''} need updating</span>
      <span style="display:flex;gap:10px;align-items:center;">
        <span class="dim mono" style="font-size:11px;">target · ${esc(d.expected||'')}</span>
        <button onclick="updateAllNodes()" ${ready?'':'disabled'} style="background:#b8860b;color:#000;font-weight:600;">⬆ Update all ${ready?`(${ready} ready)`:''}</button>
      </span></div>`;
  }

  // cards, outdated first (already sorted server-side)
  el.innerHTML = hosts.map(h=>{
    const roleChip = r=>`<span style="background:#222;color:#aaa;font-size:10px;padding:1px 7px;border-radius:8px;text-transform:capitalize;">${r}</span>`;
    const chips = (h.colocated?['colocated']:h.roles).map(roleChip).join(' ');

    let border, status;
    if(!h.reachable){ border='#555'; status='<span style="color:#888;">● unreachable</span>'; }
    else if(h.up_to_date){ border='#2c5'; status=`<span style="color:#6ee7a7;">✓ up to date · <span class="mono">${esc(h.build||'')}</span></span>`; }
    else { border='#b8860b'; status=`<span style="color:#fbbf24;">⬆ <span class="mono">${esc(h.build||'unknown')}</span> → <span class="mono">${esc(d.expected||'')}</span></span>`; }

    // ssh line
    let ssh;
    if(h.has_creds) ssh=`<span style="color:#6ee7a7;font-size:11px;">🔑 ready${h.ssh_user?` · ${esc(h.ssh_user)}@${esc(h.host)}`:''}</span>`;
    else ssh=`<span style="color:#888;font-size:11px;">🔒 no SSH saved — <a href="#" onclick="switchTab('deploy');return false;" style="color:#8af;">set up in Deploy</a></span>`;

    // action button
    let btn;
    const bid=`upbtn-${h.kind}-${h.target_id}`;
    if(!h.reachable){
      btn=`<button disabled title="Host is unreachable">Unreachable</button>`;
    } else if(!h.has_creds){
      btn=`<button disabled title="Save SSH credentials via a Push update first">Needs SSH</button>`;
    } else if(h.up_to_date){
      btn=`<button id="${bid}" class="ghost" onclick="updateNode('${h.kind}','${h.target_id}','${esc(h.host)}',this)">Re-push</button>`;
    } else {
      btn=`<button id="${bid}" onclick="updateNode('${h.kind}','${h.target_id}','${esc(h.host)}',this)" style="background:#b8860b;color:#000;font-weight:600;">⬆ Update</button>`;
    }

    return `<div class="card" style="border-left:3px solid ${border};margin-bottom:12px;">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:12px;flex-wrap:wrap;">
        <div>
          <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
            <strong class="mono">${esc(h.host)}</strong>
            ${h.name?`<span class="dim" style="font-size:12px;">${esc(h.name)}</span>`:''}
            ${chips}
          </div>
          <div style="margin-top:5px;font-size:13px;">${status}</div>
          <div style="margin-top:3px;">${ssh}</div>
        </div>
        <div>${btn}</div>
      </div>
    </div>`;
  }).join('');
}

async function _streamUpdate(url, body){
  const con=$('updates-console');
  con.style.display='block';
  con.textContent += (con.textContent?'\n':'') + '── '+new Date().toLocaleTimeString()+' ──\n';
  window._updateRunning=true;
  try{
    const res=await fetch(url,{method:'POST',credentials:'include',
      headers:{'Content-Type':'application/json'}, body:body?JSON.stringify(body):undefined});
    const reader=res.body.getReader(), dec=new TextDecoder();
    while(true){ const {done,value}=await reader.read(); if(done) break;
      con.textContent+=dec.decode(value); con.scrollTop=con.scrollHeight; }
  }catch(e){ con.textContent+='\n[stream error] '+e+'\n'; }
  window._updateRunning=false;
}

async function updateNode(kind, targetId, host, btnEl){
  if(btnEl){ btnEl.disabled=true; btnEl.dataset.label=btnEl.textContent; btnEl.textContent='Updating…'; }
  await _streamUpdate('/api/update-node',{kind, target_id:targetId});
  // give health loop a moment to re-read the build, then refresh
  setTimeout(loadUpdates, 1500);
}

async function updateAllNodes(){
  if(!confirm('Push the latest build to every machine that has saved SSH credentials?')) return;
  await _streamUpdate('/api/update-all', null);
  setTimeout(loadUpdates, 1500);
}

async function runUploadNow() {
  const r=await api('/api/transfer/run-now',{method:'POST'});
  if(r&&r.ok) { setTimeout(loadFiles, 1500); }
}

async function queueTransfer(backendPk, path) {
  const fname=path.split('/').pop();
  const r=await api(`/api/transfer/queue?backend_pk=${encodeURIComponent(backendPk)}&path=${encodeURIComponent(path)}`,
    {method:'POST'});
  if (r&&r.ok) loadFiles();
}

async function viewTranscript(filename) {
  $('ts-modal-title').textContent=filename;
  $('ts-modal-body').textContent='Loading…';
  openModal('modal-transcript');
  const r=await api(`/api/transcript-view?filename=${encodeURIComponent(filename)}`);
  if (!r||!r.ok){$('ts-modal-body').textContent='Transcript not available.';return;}
  $('ts-modal-body').textContent=await r.text();
}

async function deleteFile(bpk, path, storageSid) {
  if (storageSid) {
    if (!confirm('Delete this file from STORAGE permanently?\nThis may be the only remaining copy.\n\n' + path)) return;
    const r=await api('/api/files/delete',{method:'POST',body:JSON.stringify({storage_sid:storageSid,path})});
    if (r&&r.status===204) loadFiles();
    return;
  }
  if (!confirm('Delete this file from the recorder?\n' + path)) return;
  const r=await api('/api/files/delete',{method:'POST',body:JSON.stringify({backend_pk:bpk,path})});
  if (r&&r.status===204) loadFiles();
}

// ── Search ───────────────────────────────────────────────────────
async function loadSearchIndex() {
  const el = $('sq-index'); if (!el) return;
  try {
    const r = await api('/api/transcript-index/status');
    if (!r) return;
    const d = await r.json();
    if (!d.enabled) { el.innerHTML = '<span class="dim">Live search (catalog index off).</span>'; return; }
    el.innerHTML = `Search index: <strong>${d.indexed}</strong> transcript${d.indexed===1?'':'s'} `
      + `· <a href="#" onclick="reindexTranscripts(event)" style="color:#6ee7a7;">Reindex</a>`;
  } catch(_) {}
}

async function reindexTranscripts(ev) {
  if (ev) ev.preventDefault();
  const el = $('sq-index'); if (el) el.textContent = 'Reindexing…';
  try {
    const r = await api('/api/transcript-index/run-now', {method:'POST'});
    if (r) { const d = await r.json(); }
  } catch(_) {}
  loadSearchIndex();
}

async function doSearch() {
  const q = $('sq-input').value.trim();
  if (!q) return;
  const meta = $('sq-meta'), el = $('search-content');
  meta.textContent = 'Searching…'; el.innerHTML = '';

  const r = await api(`/api/transcript-search?q=${encodeURIComponent(q)}`);
  if (!r) { meta.textContent = ''; return; }
  const results = await r.json();

  if (!results.length) {
    meta.textContent = '';
    el.innerHTML = `<div class="empty">No transcripts match <strong>${esc(q)}</strong>.</div>`;
    return;
  }

  meta.textContent = `${results.length} result${results.length===1?'':'s'} for "${q}"`;

  // Highlight the query term in snippets
  const re = new RegExp('(' + q.replace(/[.*+?^${}()|[\]\\]/g,'\\$&') + ')', 'gi');
  el.innerHTML = `<table>
    <thead><tr><th>Creator</th><th>File</th><th>Match</th><th>Server</th><th></th></tr></thead>
    <tbody>${results.map(r => {
      const snippet = esc(r.snippet).replace(re, '<mark style="background:#4a3800;color:#fc6;border-radius:2px;padding:0 2px;">$1</mark>');
      return `<tr>
        <td><strong>${esc(r.username)}</strong></td>
        <td class="mono dim" style="font-size:11px;">${esc(r.filename)}</td>
        <td style="max-width:480px;font-size:12px;line-height:1.5;color:#c0c0c0;">${snippet}</td>
        <td class="dim" style="font-size:11px;">${esc(r.storage_label||'—')}</td>
        <td style="white-space:nowrap;"><button onclick="viewTranscript(${JSON.stringify(r.filename)})"
              style="background:#1f3a2a;color:#6ee7a7;border:1px solid #2d5a3f;padding:3px 8px;
                     border-radius:3px;font-size:11px;cursor:pointer;">View</button>
          <button onclick="gotoFile(${JSON.stringify(r.filename)})" title="Show this recording in the Files tab"
              style="background:#1a2a3a;color:#9cf;border:1px solid #2a4a6a;padding:3px 8px;
                     border-radius:3px;font-size:11px;cursor:pointer;margin-left:2px;">→ File</button></td>
      </tr>`;}).join('')}</tbody></table>`;
}

// ── Chat logs ─────────────────────────────────────────────────────
let _chatEvents = [];

function _fmtRel(sec) {
  if (sec==null) return '';
  sec = Math.max(0, Math.round(sec));
  const m = Math.floor(sec/60), s = sec%60;
  return `${m}:${String(s).padStart(2,'0')}`;
}

async function loadChat() {
  const el = $('chat-content'); $('cq-meta').textContent='';
  el.innerHTML = '<div class="dim" style="padding:10px;">Loading chat logs…</div>';
  const r = await api('/api/chat/files');
  if (!r || !r.ok) { el.innerHTML='<div class="empty">Could not load chat logs.</div>'; return; }
  const files = await r.json();
  if (!files.length) { el.innerHTML='<div class="empty">No chat logs yet. Chat is captured for watchers with 💬 on.</div>'; return; }
  el.innerHTML = `<table>
    <thead><tr><th>Creator</th><th>Log</th><th style="text-align:right;">Events</th>
      <th style="text-align:right;">Comments</th><th style="text-align:right;">Gifts</th>
      <th>Server</th><th>When</th><th></th></tr></thead>
    <tbody>${files.map(c => `<tr>
      <td><strong>${esc(c.username)}</strong></td>
      <td class="mono dim" style="font-size:11px;">${esc(c.filename)}</td>
      <td style="text-align:right;">${c.events??'—'}</td>
      <td style="text-align:right;">${c.comments??'—'}</td>
      <td style="text-align:right;">${c.gifts??'—'}</td>
      <td class="dim" style="font-size:11px;">${esc(c.storage_label||'—')}</td>
      <td class="dim" style="font-size:11px;">${fmtAgo(c.mtime)}</td>
      <td style="white-space:nowrap;"><button onclick='openChatLog(${JSON.stringify(c.filename)},${JSON.stringify(c.username)})'
            style="background:#1f3a2a;color:#6ee7a7;border:1px solid #2d5a3f;padding:3px 8px;
                   border-radius:3px;font-size:11px;cursor:pointer;">View</button>${
        c.recording_filename ? `<button onclick='gotoFile(${JSON.stringify(c.recording_filename)})' title="jump to the matched recording"
            style="background:#1a2a3a;color:#9cf;border:1px solid #2a4a6a;padding:3px 8px;border-radius:3px;font-size:11px;cursor:pointer;margin-left:2px;">→ recording</button>` : ''}</td>
    </tr>`).join('')}</tbody></table>`;
}

async function doChatSearch() {
  const q = $('cq-input').value.trim();
  if (!q) { loadChat(); return; }
  const meta=$('cq-meta'), el=$('chat-content');
  meta.textContent='Searching…'; el.innerHTML='';
  const r = await api(`/api/chat/search?q=${encodeURIComponent(q)}`);
  if (!r || !r.ok) { meta.textContent=''; return; }
  const results = await r.json();
  if (!results.length) { meta.textContent=''; el.innerHTML=`<div class="empty">No chat messages match <strong>${esc(q)}</strong>.</div>`; return; }
  const total = results.reduce((n,x)=>n+(x.match_count||0),0);
  meta.textContent = total
    ? `${total} match${total===1?'':'es'} across ${results.length} log${results.length===1?'':'s'} for "${q}"`
    : `${results.length} log${results.length===1?'':'s'} mention "${q}"`;
  const re = new RegExp('('+q.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')+')','gi');
  el.innerHTML = results.map(g => {
    // Live fan-out gives per-event `matches`; the FTS index gives a `snippet`.
    const body = (g.matches && g.matches.length)
      ? g.matches.map(m => `<div>${_eventLine(m, re)}</div>`).join('')
      : `<div style="color:#c0c0c0;">${esc(g.snippet||'').replace(re,'<mark style="background:#4a3800;color:#fc6;border-radius:2px;padding:0 2px;">$1</mark>')}</div>`;
    const count = (g.match_count!=null) ? `${g.match_count} match${g.match_count===1?'':'es'}`
                : (g.comments!=null ? `${g.comments} comments` : '');
    const recLink = g.recording_filename
      ? ` · <a href="#" onclick='gotoFile(${JSON.stringify(g.recording_filename)});return false;' style="color:#9cf;" title="jump to the matched recording">→ recording</a>` : '';
    return `<div class="card" style="border:1px solid #2a2a2a;border-radius:8px;padding:12px;margin-bottom:10px;">
      <div style="display:flex;justify-content:space-between;align-items:baseline;">
        <strong>${esc(g.username)}</strong>
        <span class="dim" style="font-size:11px;">${esc(g.filename)}${count?' · '+count:''}
          · <a href="#" onclick='openChatLog(${JSON.stringify(g.filename)},${JSON.stringify(g.username)});return false;' style="color:#6ee7a7;">open log</a>${recLink}</span>
      </div>
      <div style="margin-top:6px;font-size:12px;line-height:1.6;">${body}</div></div>`;
  }).join('');
}

const _ICON = {comment:'💬',gift:'🎁',like:'👍',join:'➡️',share:'↗️',follow:'⭐',
               subscribe:'✨',connect:'🟢',disconnect:'🔴'};

function _eventLine(ev, re) {
  const t = ev.type, icon = _ICON[t]||'·';
  const who = ev.nickname || ev.user || '';
  const whoStr = who ? `<strong>${esc(who)}</strong>${ev.user&&ev.nickname?` <span class="dim">@${esc(ev.user)}</span>`:''}` : '';
  let body = '';
  if (t==='comment') body = esc(ev.text||'');
  else if (t==='gift') body = `sent <em>${esc(ev.gift||'gift')}</em>${ev.repeat?` ×${ev.repeat}`:''}`;
  else if (t==='like') body = `liked${ev.count?` (${ev.count})`:''}`;
  else if (t==='join') body = 'joined';
  else if (t==='share') body = 'shared';
  else if (t==='follow') body = 'followed';
  else if (t==='subscribe') body = 'subscribed';
  else if (t==='connect') body = '<span class="dim">— stream connected —</span>';
  else if (t==='disconnect') body = '<span class="dim">— stream ended —</span>';
  if (re && body) body = body.replace(re,'<mark style="background:#4a3800;color:#fc6;border-radius:2px;padding:0 2px;">$1</mark>');
  return `<span class="dim" style="font-size:11px;">[${_fmtRel(ev.rel)}]</span> ${icon} ${whoStr}${whoStr&&body?': ':''}${body}`;
}

async function openChatLog(filename, username) {
  $('chat-title').textContent = `${username} — chat`;
  $('chat-filter').value=''; $('chat-typefilter').value='';
  $('chat-events').innerHTML='<div class="dim">Loading…</div>';
  openModal('modal-chat');
  const r = await api(`/api/chat/view?filename=${encodeURIComponent(filename)}`);
  if (!r || !r.ok) { $('chat-events').innerHTML='<div class="dim">Could not load this log.</div>'; return; }
  const data = await r.json();
  _chatEvents = data.events || [];
  renderChatEvents(data.truncated);
}

function renderChatEvents(truncated) {
  const ql = $('chat-filter').value.trim().toLowerCase();
  const tf = $('chat-typefilter').value;
  let evs = _chatEvents;
  if (tf) evs = evs.filter(e=>e.type===tf);
  if (ql) evs = evs.filter(e=>[e.text,e.nickname,e.user,e.gift].some(v=>String(v||'').toLowerCase().includes(ql)));
  const box = $('chat-events');
  if (!evs.length) { box.innerHTML='<div class="dim" style="padding:8px;">No matching events.</div>'; return; }
  box.innerHTML = evs.map(e=>`<div style="padding:2px 0;font-size:13px;line-height:1.6;">${_eventLine(e)}</div>`).join('')
    + (truncated?'<div class="dim" style="padding:6px 0;font-size:11px;">… log truncated (very large)</div>':'');
}


async function openCookies(pk, label) {
  _cookieBackendPk = pk;
  $('ck-title').textContent = `TikTok cookies — ${label}`;
  $('ck-status').textContent = '';
  $('ck-sessionid').value = '';
  $('ck-idc').value = '';
  $('ck-raw').value = '';
  openModal('modal-cookies');
  const r = await api(`/api/backends/${pk}/cookies`);
  if (r && r.ok) {
    const data = await r.json();
    $('ck-sessionid').value = data.sessionid_ss || '';
    $('ck-idc').value       = data['tt-target-idc'] || '';
    $('ck-raw').value       = Object.keys(data).length ? JSON.stringify(data, null, 2) : '';
  } else {
    $('ck-status').style.color = '#fc6';
    $('ck-status').textContent = 'Could not reach backend to load current cookies.';
  }
}

async function saveCookies() {
  const s = $('ck-status');
  s.style.color = '#9aa'; s.textContent = 'Saving…';
  let body = {};
  const rawVal = $('ck-raw').value.trim();
  if (rawVal) {
    try { body = JSON.parse(rawVal); }
    catch(_) { s.style.color = '#f99'; s.textContent = 'Invalid JSON in raw field.'; return; }
  } else {
    const sid = $('ck-sessionid').value.trim();
    const idc = $('ck-idc').value.trim() || 'useast2a';
    if (!sid) { s.style.color = '#f99'; s.textContent = 'sessionid_ss is required.'; return; }
    body = { sessionid_ss: sid, 'tt-target-idc': idc };
  }
  const r = await api(`/api/backends/${_cookieBackendPk}/cookies`,
    {method:'POST', body:JSON.stringify(body)});
  if (r && r.ok) {
    s.style.color = '#6ee7a7';
    s.textContent = 'Saved. Cookies apply on the next recording session.';
  } else {
    s.style.color = '#f99';
    s.textContent = 'Save failed — check the backend is reachable.';
  }
}

// ── Storage ──────────────────────────────────────────────────────
function stTogglePw() {
  const i = $('st-token'); i.type = i.type === 'password' ? 'text' : 'password';
}

function fmtDisk(b) {
  if (b == null) return '—';
  const gb = b / 1073741824;
  return gb >= 1 ? gb.toFixed(1) + ' GB' : (b/1048576).toFixed(0) + ' MB';
}

// ── Storage usage breakdown ──────────────────────────────────────
let _breakdownShown = false;

function _fmtGB(b) { return b==null ? '—' : (b/1073741824).toFixed(1)+' GB'; }

function transcriptionPanel(s) {
  const active = s.active || [];
  const conc = s.concurrency;
  const head = (conc!=null)
    ? `<div class="dim" style="font-size:12px;margin-top:8px;">🎙 Transcription · up to ${conc} at once${s.queue_depth?` · ${s.queue_depth} queued`:''}</div>`
    : '';
  if (!active.length)
    return head + (conc!=null ? '<div class="dim" style="font-size:12px;">idle</div>' : '');
  const rows = active.map(a => {
    const pct = Math.round((a.pct||0)*100);
    const el = a.elapsed!=null ? `${Math.floor(a.elapsed/60)}m${Math.round(a.elapsed%60)}s` : '';
    const dur = a.duration ? ` of ${Math.round(a.duration)}s audio` : '';
    const stalled = a.stalled;
    const barColor = stalled ? '#e5534b' : '#3fb950';
    return `<div style="margin:6px 0;">
      <div style="display:flex;justify-content:space-between;font-size:12px;">
        <span>${esc(a.filename)} ${stalled?'<span style="color:#e5534b;font-weight:600;">⚠ STALLED</span>':''}</span>
        <span class="dim">${pct}% · ${el}${dur?` · ${Math.round(a.processed_sec||0)}s${dur}`:''}</span>
      </div>
      <div style="background:#222;border-radius:4px;height:6px;overflow:hidden;margin-top:2px;">
        <div style="height:6px;width:${pct}%;background:${barColor};transition:width .4s;"></div>
      </div>
    </div>`;
  }).join('');
  return head + rows;
}

async function toggleBreakdown(btn) {
  const box = $('storage-breakdown');
  _breakdownShown = !_breakdownShown;
  box.style.display = _breakdownShown ? '' : 'none';
  if (window._breakdownTimer) { clearInterval(window._breakdownTimer); window._breakdownTimer = null; }
  if (!_breakdownShown) return;
  box.innerHTML = '<div class="dim" style="padding:10px;">Loading usage…</div>';
  await loadBreakdown();
  // Auto-refresh while open so transcription progress updates live.
  window._breakdownTimer = setInterval(() => { if (_breakdownShown) loadBreakdown(); }, 5000);
}

async function loadBreakdown() {
  const box = $('storage-breakdown');
  const r = await api('/api/storage-breakdown');
  if (!r || !r.ok) { box.innerHTML = '<div class="dim">Could not load breakdown.</div>'; return; }
  const d = await r.json();
  if (!d.servers.length) { box.innerHTML = '<div class="empty">No storage servers.</div>'; return; }
  box.innerHTML = d.servers.map(s => {
    const usedPct = (s.disk_total && s.disk_free!=null)
      ? Math.round((1 - s.disk_free/s.disk_total)*100) : null;
    const bar = usedPct!=null
      ? `<div style="background:#222;border-radius:4px;height:8px;overflow:hidden;margin:6px 0;">
           <div style="height:8px;width:${usedPct}%;background:${usedPct>85?'#e5534b':usedPct>70?'#d9a23b':'#3fb950'};"></div>
         </div>
         <div class="dim" style="font-size:12px;">${usedPct}% disk used · ${_fmtGB(s.disk_free)} free of ${_fmtGB(s.disk_total)}</div>`
      : '<div class="dim" style="font-size:12px;">disk usage unknown</div>';
    const rows = s.creators.length
      ? s.creators.map(c => `<tr>
          <td>${esc(c.username)}</td>
          <td style="text-align:right;">${c.files}</td>
          <td style="text-align:right;">${fmtBytes(c.bytes)}</td>
          <td style="text-align:right;"><button class="ghost" style="font-size:11px;padding:2px 8px;"
            onclick='openMove(${JSON.stringify(s.id)},"creator",${JSON.stringify(c.username)},${JSON.stringify(s.label)})'>Move →</button>
            <a href="#" style="font-size:11px;color:#888;margin-left:6px;"
            onclick='browseFiles(${JSON.stringify(s.id)},${JSON.stringify(c.username)},${JSON.stringify(s.label)});return false;'>pick</a></td>
        </tr>`).join('')
      : '<tr><td colspan="4" class="dim">No recordings on this server.</td></tr>';
    return `<div class="card" style="border:1px solid #2a2a2a;border-radius:8px;padding:14px;margin-bottom:12px;">
      <div style="display:flex;justify-content:space-between;align-items:baseline;">
        <strong>${esc(s.label)} ${s.build?`<span class="dim" style="font-size:10px;font-weight:400;">worker ${esc(s.build)}</span>`:`<span style="font-size:10px;color:#e5534b;">⚠ old worker — push-update storage</span>`}</strong>
        <span class="dim" style="font-size:12px;">${s.healthy?'':'⚠ offline · '}${s.file_count} files · ${fmtBytes(s.recordings_bytes)} recordings
          ${s.file_count?` · <a href="#" onclick='openMove(${JSON.stringify(s.id)},"server",null,${JSON.stringify(s.label)});return false;' style="color:#6ee7a7;">drain →</a>`:''}</span>
      </div>
      ${bar}
      ${s.archive_remote ? `<div class="dim" style="font-size:12px;margin-top:4px;">☁ archiving to <code>${esc(s.archive_remote)}</code> · ${s.archived_count||0} archived</div>` : ''}
      ${transcriptionPanel(s)}
      <table style="margin-top:8px;width:100%;">
        <thead><tr><th>Creator</th><th style="text-align:right;">Files</th><th style="text-align:right;">Size</th><th></th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`;
  }).join('');
}

// ── Moves (between storage servers) ───────────────────────────────
let _moveCtx = null;
let _browseCtx = null;

async function browseFiles(srcId, username, srcLabel) {
  _browseCtx = {srcId, username};
  $('browse-title').textContent = `${username} — files on ${srcLabel}`;
  $('browse-err').textContent = '';
  $('browse-list').innerHTML = '<div class="dim">Loading…</div>';
  // destinations
  const sr = await api('/api/storage');
  const dests = (sr && sr.ok) ? (await sr.json()).filter(s=>s.id!==srcId) : [];
  $('browse-dest').innerHTML = dests.length
    ? dests.map(s=>`<option value="${s.id}">${esc(s.label||s.url)}</option>`).join('')
    : '<option value="">No other storage servers</option>';
  openModal('modal-browse');
  const r = await api(`/api/storage/${srcId}/files?username=${encodeURIComponent(username)}`);
  if (!r || !r.ok) { $('browse-list').innerHTML='<div class="dim">Could not list files.</div>'; return; }
  const files = await r.json();
  if (!files.length) { $('browse-list').innerHTML='<div class="dim">No files.</div>'; return; }
  const KIND={recording:'🎬',transcript:'📄',chat:'💬'};
  $('browse-list').innerHTML = files.map(f=>`<label style="display:flex;gap:8px;align-items:center;font-size:12px;padding:3px 0;">
    <input type="checkbox" class="browse-cb" value="${esc(f.path)}" style="width:auto;"/>
    <span>${KIND[f.kind]||'·'} ${esc(f.filename)}</span>
    <span class="dim" style="margin-left:auto;">${fmtBytes(f.size_bytes)}</span></label>`).join('');
}

async function moveSelected() {
  const dst = $('browse-dest').value;
  if (!dst) { $('browse-err').textContent='No destination available.'; return; }
  const paths = Array.from(document.querySelectorAll('.browse-cb:checked')).map(c=>c.value);
  if (!paths.length) { $('browse-err').textContent='Select at least one file.'; return; }
  const r = await api('/api/moves', {method:'POST', body:JSON.stringify(
    {src_storage_id:_browseCtx.srcId, dst_storage_id:dst, scope:'files', files:paths})});
  if (!r || !r.ok) { $('browse-err').textContent='Could not start move.'; return; }
  const d = await r.json();
  closeModal('modal-browse');
  if ($('moves-panel').style.display==='none' || !$('moves-panel').style.display) toggleMoves(); else loadMoves();
  alert(`Started copying ${d.created} file(s). Originals kept until you confirm deletion in the Moves panel.`);
}

async function openMove(srcId, scope, username, srcLabel) {
  _moveCtx = {srcId, scope, username};
  $('move-err').textContent = '';
  $('move-title').textContent = scope==='server' ? `Drain "${srcLabel}"`
    : scope==='creator' ? `Move ${username}'s files` : 'Move files';
  $('move-desc').textContent = scope==='server'
    ? `Move every recording, transcript and chat log off "${srcLabel}" to another server.`
    : scope==='creator' ? `Move all of ${username}'s files from "${srcLabel}".`
    : 'Move the selected files.';
  // destinations = all storage servers except the source
  const r = await api('/api/storage');
  const dests = (r && r.ok) ? (await r.json()).filter(s => s.id !== srcId) : [];
  const sel = $('move-dest');
  if (!dests.length) { sel.innerHTML = '<option value="">No other storage servers</option>'; }
  else sel.innerHTML = dests.map(s => `<option value="${s.id}">${esc(s.label||s.url)}</option>`).join('');
  openModal('modal-move');
}

async function submitMove(files) {
  const dst = $('move-dest').value;
  if (!dst) { $('move-err').textContent = 'No destination available.'; return; }
  const body = {src_storage_id:_moveCtx.srcId, dst_storage_id:dst, scope:_moveCtx.scope};
  if (_moveCtx.scope === 'creator') body.username = _moveCtx.username;
  if (_moveCtx.scope === 'files') body.files = files || _moveCtx.files || [];
  const r = await api('/api/moves', {method:'POST', body:JSON.stringify(body)});
  if (!r || !r.ok) { $('move-err').textContent = 'Could not start move.'; return; }
  const d = await r.json();
  closeModal('modal-move');
  if (!$('moves-panel').style.display || $('moves-panel').style.display==='none') toggleMoves();
  else loadMoves();
  alert(`Started copying ${d.created} file(s). Watch progress in the Moves panel; originals are kept until you confirm deletion.`);
}

let _movesShown = false;
async function toggleMoves(btn) {
  _movesShown = !_movesShown;
  $('moves-panel').style.display = _movesShown ? '' : 'none';
  if (window._movesTimer){clearInterval(window._movesTimer);window._movesTimer=null;}
  if (!_movesShown) return;
  await loadMoves();
  window._movesTimer = setInterval(()=>{ if(_movesShown) loadMoves(); }, 4000);
}

async function loadMoves() {
  const box = $('moves-panel');
  const r = await api('/api/moves');
  if (!r || !r.ok) { box.innerHTML = '<div class="dim">Could not load moves.</div>'; return; }
  const moves = await r.json();
  if (!moves.length) { box.innerHTML = '<div class="card" style="padding:12px;"><div class="dim">No moves yet. Use “Move →” on a creator or “drain →” on a server in the usage breakdown.</div></div>'; return; }
  // group by batch
  const batches = {};
  for (const m of moves) { (batches[m.batch] = batches[m.batch] || []).push(m); }
  const ICON={pending:'⏳',copying:'⏳',copied:'✅',done:'🗑️',error:'⚠️'};
  let html = '<div class="card" style="padding:12px;"><div style="display:flex;justify-content:space-between;align-items:center;"><strong>Moves</strong>'
    + '<button class="ghost" style="font-size:11px;" onclick="clearMoves()">Clear finished</button></div>';
  for (const [batch, items] of Object.entries(batches)) {
    const c = items[0];
    const users = [...new Set(items.map(m=>m.username).filter(Boolean))];
    const userLabel = users.length === 0 ? ''
      : users.length === 1 ? esc(users[0])
      : (users.length <= 3 ? users.map(esc).join(', ') : `${users.length} creators`);
    const counts = items.reduce((a,m)=>{a[m.status]=(a[m.status]||0)+1;return a;},{});
    const copied = (counts.copied||0), total = items.length;
    const allDone = items.every(m=>m.status==='done');
    const verified = items.filter(m=>m.status==='copied');
    const inflight = (counts.pending||0)+(counts.copying||0);
    html += `<div style="border:1px solid #262626;border-radius:6px;padding:10px;margin-top:10px;">
      <div style="display:flex;justify-content:space-between;align-items:baseline;">
        <span><strong>${esc(c.src_label)}</strong> → <strong>${esc(c.dst_label)}</strong>
          <span class="dim" style="font-size:12px;">${userLabel?`· ${userLabel}`:''} · ${total} file(s)</span></span>
        <span class="dim" style="font-size:12px;">${Object.entries(counts).map(([k,v])=>`${ICON[k]||''}${v} ${k}`).join(' · ')}</span>
      </div>
      ${inflight?`<div class="dim" style="font-size:12px;margin-top:4px;">copying… ${copied}/${total} verified</div>`:''}
      ${counts.error?`<div style="color:#e5534b;font-size:12px;margin-top:4px;">${items.filter(m=>m.error).slice(0,2).map(m=>esc(m.filename+': '+m.error)).join('<br>')}
        <button class="ghost" style="font-size:11px;margin-left:6px;" onclick="retryMoves('${batch}')">Retry failed</button></div>`:''}
      ${verified.length?`<button class="primary" style="font-size:12px;margin-top:8px;padding:4px 10px;"
          onclick="confirmDeleteMove('${batch}', ${verified.length})">Confirm delete ${verified.length} source file(s)</button>`:''}
      ${allDone?'<div style="color:#6ee7a7;font-size:12px;margin-top:6px;">✓ moved &amp; source removed</div>':''}
    </div>`;
  }
  html += '</div>';
  box.innerHTML = html;
}

async function confirmDeleteMove(batch, n) {
  if (!confirm(`Permanently delete ${n} source file(s) that are verified on the destination? This frees space on the source server.`)) return;
  const r = await api('/api/moves/confirm-delete', {method:'POST', body:JSON.stringify({batch})});
  if (r && r.ok) { loadMoves(); if(_breakdownShown) loadBreakdown(); }
  else alert('Delete failed.');
}

async function clearMoves() {
  const r = await api('/api/moves/clear', {method:'POST', body:JSON.stringify({what:'done'})});
  if (r && r.ok) loadMoves();
}

async function retryMoves(batch) {
  const r = await api('/api/moves/retry', {method:'POST', body:JSON.stringify({batch})});
  if (r && r.ok) loadMoves();
}

async function downloadBackup(btn) {
  if (btn) { btn.disabled = true; btn.textContent = 'Backing up…'; }
  try {
    const resp = await fetch('/api/backup', {credentials:'include'});
    if (!resp.ok) { alert('Backup failed: HTTP ' + resp.status); return; }
    const blob = await resp.blob();
    const cd = resp.headers.get('Content-Disposition') || '';
    const m = cd.match(/filename="([^"]+)"/);
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = m ? m[1] : 'tt-control-backup.sqlite';
    document.body.appendChild(a); a.click();
    a.remove(); URL.revokeObjectURL(a.href);
  } catch (e) {
    alert('Backup failed: ' + e.message);
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = '⬇ Backup DB'; }
  }
}

// ── Routing & retention ──────────────────────────────────────────
let _routingData = null;

function _delControls(prefix, mode, delaySec) {
  // delay value shown in hours for convenience
  const hrs = delaySec ? (delaySec/3600) : 24;
  return `
    <select id="${prefix}-mode" onchange="onRoutingModeChange('${prefix}')">
      <option value="immediate" ${mode==='immediate'?'selected':''}>delete immediately after upload</option>
      <option value="delay" ${mode==='delay'?'selected':''}>delete after a delay</option>
      <option value="never" ${mode==='never'?'selected':''}>never delete (keep on recorder)</option>
    </select>
    <span id="${prefix}-delaywrap" style="margin-left:6px;${mode==='delay'?'':'display:none;'}">
      <input id="${prefix}-delay" type="number" min="0" step="0.5" value="${hrs}"
        style="width:64px;" /> hours
    </span>`;
}

function onRoutingModeChange(prefix) {
  const mode = $(`${prefix}-mode`).value;
  const w = $(`${prefix}-delaywrap`);
  if (w) w.style.display = mode === 'delay' ? '' : 'none';
}

function _storageOptions(sel) {
  const auto = `<option value="" ${!sel?'selected':''}>auto (most free disk)</option>`;
  const opts = _routingData.storages.map(s =>
    `<option value="${s.id}" ${sel===s.id?'selected':''}>${esc(s.label||s.url)}${s.healthy?'':' (offline)'}</option>`
  ).join('');
  return auto + opts;
}

function _host(u){ try { return new URL(u).hostname; } catch(_){ return u; } }

function _ruleRow(label, key, rule, badge) {
  const p = 'r_' + key.replace(/[^a-zA-Z0-9]/g,'_');
  const tgt = rule ? rule.target_storage_pk : null;
  const mode = rule ? rule.delete_mode : 'immediate';
  const delay = rule ? rule.delete_delay_sec : 0;
  const ts = tgt && _routingData.storages.find(s=>s.id===tgt);
  const warn = ts && !ts.healthy
    ? ' <span title="this target storage is currently offline" style="color:#fc6;">⚠ offline</span>' : '';
  return `<tr>
    <td><strong>${esc(label)}</strong>${badge||''}</td>
    <td><select id="${p}-target">${_storageOptions(tgt)}</select>${warn}</td>
    <td>${_delControls(p, mode, delay)}</td>
    <td style="white-space:nowrap;">
      <button class="primary" style="font-size:12px;padding:4px 10px;"
        onclick="saveRouting('${esc(key)}','${p}')">Save</button>
      ${key!=='*' ? `<button class="ghost" style="font-size:12px;padding:4px 10px;"
        onclick="clearRouting('${esc(key)}')" title="Revert to global default">Reset</button>` : ''}
    </td>
  </tr>`;
}

async function loadRouting() {
  const r = await api('/api/routing'); if (!r) return;
  _routingData = await r.json();
  const el = $('routing-content');
  if (!_routingData.storages.length) {
    el.innerHTML = '<div class="empty">Add a storage server first — routing needs somewhere to send recordings.</div>';
    return;
  }
  let html = `<h3 style="margin:4px 0 8px;font-size:14px;">Global default</h3>
    <table><thead><tr><th>Applies to</th><th>Send to</th><th>Recorder retention</th><th></th></tr></thead>
    <tbody>${_ruleRow('All backends (default)', '*', _routingData.default)}</tbody></table>`;

  if (_routingData.backends.length) {
    html += `<h3 style="margin:18px 0 8px;font-size:14px;">Per-backend overrides</h3>
      <table><thead><tr><th>Backend</th><th>Send to</th><th>Recorder retention</th><th></th></tr></thead><tbody>`;
    const shosts = new Set(_routingData.storages.map(s => _host(s.url)));
    html += _routingData.backends.map(b => {
      const rule = _routingData.rules[b.id];
      const label = `${b.backend_id}${b.region?' ('+b.region+')':''}`;
      const colo = shosts.has(_host(b.url))
        ? ' <span title="this recorder shares a disk with a storage server — its recordings are already on storage, so retention here is moot" style="background:#13302b;color:#6ee7a7;font-size:10px;padding:1px 6px;border-radius:3px;margin-left:6px;">colocated</span>'
        : '';
      return _ruleRow(label, b.id, rule, colo);
    }).join('');
    html += `</tbody></table>`;
  }
  el.innerHTML = html;
}

async function saveRouting(key, prefix) {
  const mode = $(`${prefix}-mode`).value;
  let delaySec = 0;
  if (mode === 'delay') {
    const hrs = parseFloat($(`${prefix}-delay`).value);
    if (isNaN(hrs) || hrs < 0) { alert('Enter a valid delay in hours'); return; }
    delaySec = Math.round(hrs * 3600);
  }
  const body = {
    backend_pk: key,
    target_storage_pk: $(`${prefix}-target`).value || null,
    delete_mode: mode,
    delete_delay_sec: delaySec,
  };
  const r = await api('/api/routing', {method:'POST', body:JSON.stringify(body)});
  if (r && r.ok) { loadRouting(); }
  else { alert('Save failed'); }
}

async function clearRouting(key) {
  const r = await api('/api/routing/' + encodeURIComponent(key), {method:'DELETE'});
  if (r && r.ok) loadRouting();
}

// ── Cloud archive (3rd hop) ──────────────────────────────────────
let _archiveServers = [];   // last /api/archive/overview result

function onArchiveProviderChange() {
  const p = $('ar-provider').value;
  ['s3','b2','dropbox','filen','raw'].forEach(x =>
    $('ar-fields-'+x).style.display = (x===p) ? '' : 'none');
}

// Load the Archive tab: existing per-server config overview + the config form.
async function loadArchive() {
  onArchiveProviderChange();
  const ov = $('archive-overview');
  let servers = [];
  try {
    const r = await api('/api/archive/overview');
    if (r && r.ok) servers = (await r.json()).servers || [];
  } catch(_) {}
  _archiveServers = servers;

  // Server picker for the form
  const sel = $('ar-server');
  if (sel) {
    const prev = sel.value;
    sel.innerHTML = servers.length
      ? servers.map(s=>`<option value="${s.id}">${esc(s.label)}</option>`).join('')
      : '<option value="">No storage servers</option>';
    if (prev && servers.some(s=>s.id===prev)) sel.value = prev;
    onArchiveServerSelect();
  }

  if (ov) {
    if (!servers.length) {
      ov.innerHTML = '<div class="empty">No storage servers yet — add one in the Servers tab.</div>';
    } else {
      ov.innerHTML = `<table>
        <thead><tr><th>Storage server</th><th>Status</th><th>Remote</th><th>Archives</th>
          <th>Reclaim disk</th><th>Last evict</th><th></th></tr></thead>
        <tbody>${servers.map(s=>{
          const off = !s.healthy ? '<span class="dim"> · offline</span>' : '';
          let status;
          if (!s.healthy) status = '<span style="color:#888;">unreachable</span>';
          else if (s.configured) status = '<span style="color:#6ee7a7;">✓ archiving</span>';
          else status = '<span style="color:#888;">not configured</span>';
          const fail = s.archive_failed_count
            ? ` <span style="color:#f99;" title="failed archive attempts">(${s.archive_failed_count} failed)</span>` : '';
          const reclaim = s.configured
            ? (s.delete_local ? `evict &gt; ${s.evict_high_pct??85}% disk` : '<span class="dim">off (keep all)</span>')
            : '—';
          return `<tr>
            <td><strong>${esc(s.label)}</strong>${off}</td>
            <td>${status}</td>
            <td class="mono dim" style="font-size:11px;">${s.remote?esc(s.remote)+(s.what?` <span class="dim">(${esc(s.what)})</span>`:''):'—'}</td>
            <td>${s.configured?(s.archived_count??0)+fail:'—'}</td>
            <td style="font-size:12px;">${reclaim}</td>
            <td class="dim" style="font-size:11px;" title="${esc(_evictTitle(s.last_evict))}">${_fmtEvict(s.last_evict)}</td>
            <td style="white-space:nowrap;"><button class="ghost" style="font-size:12px;padding:4px 10px;"
                  onclick="pickArchiveServer('${s.id}')">${s.configured?'Edit':'Set up'}</button>${
              s.configured ? `<button class="ghost" style="font-size:12px;padding:4px 10px;margin-left:4px;"
                  onclick="openEvict('${s.id}')">Eviction</button>` : ''}</td>
          </tr>`;}).join('')}</tbody></table>`;
    }
  }
}

function _fmtEvict(le) {
  if (!le || typeof le !== 'object') return '—';
  if (le.evicted) return `freed ${fmtBytes(le.freed_bytes)} · ${le.evicted} file${le.evicted===1?'':'s'}`;
  switch (le.reason) {
    case 'disabled': return 'off';
    case 'disk_below_high': return `idle · disk ${le.disk_pct_before}% < ${le.high_pct}%`;
    case 'no_eligible_files':
      if (le.archived === 0) return '⚠ nothing archived yet';
      if (le.candidates === 0 && le.skipped_young > 0) return `${le.skipped_young} archived <${le.min_age_h}h (kept hot)`;
      if (le.skipped_remote > 0) return `⚠ ${le.skipped_remote} not verified on remote`;
      return 'no eligible files';
    default: return '—';
  }
}
function _evictTitle(le) {
  if (!le || typeof le !== 'object') return '';
  return `disk ${le.disk_pct_before}% (evict >${le.high_pct}% → ${le.low_pct}%) · `
    + `${le.archived}/${le.total_mp4} archived · ${le.candidates} eligible · `
    + `${le.skipped_young} too new (<${le.min_age_h}h) · ${le.skipped_remote} unverified`;
}

function pickArchiveServer(sid) {
  const sel = $('ar-server'); if (sel) sel.value = sid;
  onArchiveServerSelect();
  $('archive-config')?.scrollIntoView({behavior:'smooth', block:'start'});
}

let _evictSid = null;
function openEvict(sid) {
  const s = _archiveServers.find(x => x.id === sid);
  if (!s) return;
  if (!s.configured) { alert('Configure the archive on this server first, then eviction can be toggled.'); return; }
  _evictSid = sid;
  $('evict-server').innerHTML = `<strong>${esc(s.label)}</strong> → <span class="mono">${esc(s.remote||'archive')}</span>`;
  $('evict-on').checked = !!s.delete_local;
  $('evict-high').value = s.evict_high_pct || 85;
  $('evict-out').style.display = 'none'; $('evict-out').textContent = '';
  openModal('modal-evict');
}

async function applyEviction() {
  if (!_evictSid) return;
  const body = {
    delete_local: $('evict-on').checked,
    evict_high_pct: parseFloat($('evict-high').value || '85'),
    evict_low_pct: parseFloat($('evict-low').value || '70'),
    evict_min_age_sec: Math.round(parseFloat($('evict-age').value || '24') * 3600),
    delete_delay_sec: 0,
  };
  const out = $('evict-out'), btn = $('evict-apply');
  out.style.display = ''; out.textContent = ''; btn.disabled = true; btn.textContent = 'Applying…';
  try {
    const resp = await fetch(`/api/storage/${_evictSid}/archive-eviction`, {method:'POST', credentials:'include',
      headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)});
    if (!resp.ok) { out.textContent += `[ERROR] HTTP ${resp.status}\n` + await resp.text(); }
    else { const rd = resp.body.getReader(), dec = new TextDecoder();
      while(true){ const {value,done} = await rd.read(); if(done) break;
        out.textContent += dec.decode(value,{stream:true}); out.scrollTop = out.scrollHeight; } }
  } catch(e) { out.textContent += '\n[ERROR] ' + e.message; }
  finally { btn.disabled = false; btn.textContent = 'Apply'; setTimeout(loadArchive, 1200); }
}

async function onArchiveServerSelect() {
  const sid = $('ar-server') ? $('ar-server').value : '';
  const cur = $('ar-current');
  if (!cur) return;
  const s = _archiveServers.find(x => x.id === sid);
  cur.innerHTML = (s && s.configured)
    ? `Currently archiving to <span class="mono">${esc(s.remote)}</span> (${s.archived_count||0} archived).`
    : 'Archiving not configured on this server yet.';
  // Expand the SSH section only on first connect (no saved creds for this host).
  const det = $('ar-ssh-details');
  if (det && s) {
    try {
      const host = new URL(s.url).hostname;
      const sc = await (await api('/api/ssh-creds/'+encodeURIComponent(host))).json();
      det.open = !(sc && sc.exists);
    } catch(_) { det.open = true; }
  }
}

async function _streamArchive(url, body, btn, label) {
  const out = $('ar-out'); out.style.display=''; out.textContent='';
  if (btn){ btn.disabled=true; btn.textContent=label; }
  try {
    const resp = await fetch(url, {method:'POST', credentials:'include',
      headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)});
    if (!resp.ok) { out.textContent += `[ERROR] HTTP ${resp.status}\n`+await resp.text(); return; }
    const reader = resp.body.getReader(), dec = new TextDecoder();
    while(true){ const {value,done}=await reader.read(); if(done)break;
      out.textContent += dec.decode(value,{stream:true}); out.scrollTop=out.scrollHeight; }
  } catch(e){ out.textContent += '\n[ERROR] '+e.message; }
  finally { if(btn){ btn.disabled=false; btn.textContent=label==='Working…'?'Configure & enable':label; } loadArchive(); }
}

function _archiveSsh() {
  return { ssh_user: $('ar-ssh-user').value.trim(), ssh_port: parseInt($('ar-ssh-port').value,10),
    auth_method: $('ar-ssh-auth').value, ssh_password: $('ar-ssh-pw').value, ssh_key_path: $('ar-ssh-key').value.trim() };
}

async function enableArchive() {
  const sid = $('ar-server').value;
  if (!sid) { alert('Select a storage server'); return; }
  const p = $('ar-provider').value;
  const body = { ...(_archiveSsh()), provider:p, remote_name:'archive',
    remote_path: $('ar-path').value.trim(), archive_what: $('ar-what').value,
    delete_local: $('ar-delete').checked, delete_delay_sec: 0,
    evict_high_pct: parseFloat($('ar-evict-high').value||'85'),
    evict_low_pct: parseFloat($('ar-evict-low').value||'70'),
    evict_min_age_sec: Math.round(parseFloat($('ar-evict-age').value||'24')*3600),
    transfers: parseInt($('ar-transfers').value||'4',10),
    bwlimit: $('ar-bwlimit').value.trim(), retry_backoff_sec: 120 };
  if (p==='s3'){ body.s3_access_key=$('ar-s3-key').value.trim(); body.s3_secret=$('ar-s3-secret').value;
    body.s3_region=$('ar-s3-region').value.trim(); body.s3_endpoint=$('ar-s3-endpoint').value.trim(); }
  else if (p==='b2'){ body.b2_account=$('ar-b2-account').value.trim(); body.b2_key=$('ar-b2-key').value; }
  else if (p==='dropbox'){ body.dropbox_token=$('ar-dbx-token').value.trim(); }
  else if (p==='filen'){ body.filen_email=$('ar-filen-email').value.trim();
    body.filen_password=$('ar-filen-pw').value; body.filen_2fa=$('ar-filen-2fa').value.trim();
    body.filen_api_key=$('ar-filen-apikey').value.trim(); }
  else if (p==='raw'){ body.raw_config=$('ar-raw').value; }
  const btn=$('ar-btn'); const lbl=btn.textContent;
  await _streamArchive(`/api/storage/${sid}/archive-config`, body, btn, 'Working…');
  btn.textContent=lbl;
}

async function testArchive() {
  const sid = $('ar-server').value;
  if (!sid) { alert('Select a storage server'); return; }
  await _streamArchive(`/api/storage/${sid}/archive-test`, _archiveSsh(), null, 'Testing…');
}

async function disableArchive() {
  const sid = $('ar-server').value;
  if (!sid) { alert('Select a storage server'); return; }
  if (!confirm('Disable cloud archiving on this server? (rclone config is kept)')) return;
  await _streamArchive(`/api/storage/${sid}/archive-disable`, _archiveSsh(), null, '');
}

async function loadStorage() {
  const r = await api('/api/storage'); if (!r) return;
  const list = await r.json();
  const el = $('storage-content');
  if (!list.length) {
    el.innerHTML = '<div class="empty">No storage servers yet. Add one below, '
      + 'or deploy a fresh one from the Deploy tab.</div>';
    return;
  }
  // Detect colocation (same hostname = same VPS also acting as a recorder).
  let recorderHosts = new Set();
  try {
    const br = await api('/api/backends');
    if (br && br.ok) (await br.json()).forEach(b => {
      try { recorderHosts.add(new URL(b.url).hostname); } catch(_){}
    });
  } catch(_) {}
  el.innerHTML = `<table>
    <thead><tr><th>Server</th><th>URL</th><th>Status</th><th>Build</th><th>Model</th>
      <th>Free disk</th><th>Queue</th><th>Transcription</th><th>Token</th><th></th></tr></thead>
    <tbody>${list.map(s => {
      const dot = s.state === 'healthy'
        ? '<span style="color:#6ee7a7;">● healthy</span>'
        : s.state === 'auth'
          ? '<span style="color:#d9a23b;">● auth failed</span> <span class="dim" style="font-size:11px;">(token rejected — Edit token)</span>'
          : '<span style="color:#f99;">● unreachable</span>';
      const build = s.build
        ? `<span class="dim" style="font-size:11px;">${esc(s.build)}</span>`
        : (s.is_reachable ? '<span style="color:#d9a23b;font-size:11px;">old</span>' : '—');
      let sHost=''; try { sHost=new URL(s.url).hostname; } catch(_){}
      const colocBadge = sHost && recorderHosts.has(sHost)
        ? `<span style="background:#1a2a3a;color:#9cf;font-size:10px;padding:1px 5px;border-radius:3px;margin-left:4px;" title="Same VPS also runs a recorder — this is one machine doing both roles">📦 colocated · ${esc(sHost)}</span>`
        : '';
      return `<tr>
        <td><strong>${esc(s.label||'—')}</strong>${colocBadge}</td>
        <td class="mono dim" style="font-size:12px;">${esc(s.url)}</td>
        <td>${dot}</td>
        <td>${build}</td>
        <td>${esc(s.model||'—')}</td>
        <td>${fmtDisk(s.disk_free_bytes)}</td>
        <td>${s.queue_depth ?? '—'}</td>
        <td>${ s.transcribe_enabled === false
                ? '<span style="color:#d9a23b;font-size:11px;" title="Storage-only: this server does not transcribe">⏸ off (storage-only)</span>'
                : s.transcribe_enabled === true
                  ? `<span style="color:#6ee7a7;font-size:11px;">▶ on</span> <span class="dim" style="font-size:11px;">×${s.concurrency ?? '?'}</span>`
                  : '<span class="dim" style="font-size:11px;">—</span>' }</td>
        <td class="mono dim" style="font-size:11px;">${esc(s.token_masked||'—')}</td>
        <td><button class="ghost" onclick="openWorkerConfig('${s.id}')">⚙ Config</button>
            <button class="ghost" onclick='openMove(${JSON.stringify(s.id)},"server",null,${JSON.stringify(s.label||s.url)})' title="Copy everything on this server to another, then confirm deletion">↔ Drain →</button>
            <button class="ghost" onclick="editStorageToken('${s.id}','${esc(s.label||s.url)}')">Edit token</button>
            <button class="danger" onclick="removeStorage('${s.id}','${esc(s.label||s.url)}')">Remove</button></td>
      </tr>`;}).join('')}</tbody></table>
    <p style="font-size:11px;color:#666;margin:8px 2px 0;">
      New recordings go to the healthy server with the most free disk.
      "Auth failed" means the server is up but the token doesn't match — use Edit token.</p>`;
}

async function editStorageToken(sid, label) {
  const token = prompt(`New token for "${label}"\n(get it on the box: grep WHISPER_AUTH_TOKEN /etc/tt-storage.env)`);
  if (!token) return;
  const r = await api(`/api/storage/${sid}/token`, {method:'POST', body:JSON.stringify({token:token.trim()})});
  if (r && r.ok) {
    const d = await r.json();
    alert(d.validated ? 'Token updated and validated ✓' : 'Token saved, but the server still rejected it — double-check the value.');
    loadStorage();
  } else alert('Update failed.');
}

let _wcfgSid=null;
async function openWorkerConfig(sid) {
  _wcfgSid=sid;
  let s=null;
  try { s=(await (await api('/api/storage')).json()).find(x=>x.id===sid); } catch(_){}
  if(!s){ alert('Could not load this server\'s settings — hit Refresh and try again.'); return; }
  $('wcfg-title').textContent=`Transcription settings — ${s.label||s.url}`;
  $('wcfg-conc').value = (s.concurrency==null?2:s.concurrency);
  $('wcfg-trans').value = (s.transcribe_enabled !== false) ? '1' : '0';
  $('wcfg-model').value = s.model || 'base';
  $('wcfg-vad').value = (s.vad===false) ? '0' : '1';
  $('wcfg-beam').value = String(s.beam_size || 5);
  $('wcfg-out').style.display='none'; $('wcfg-out').textContent='';
  // Prefill SSH from saved creds for this host (never returns the password)
  try {
    const host=new URL(s.url).hostname;
    const sc=await (await api('/api/ssh-creds/'+encodeURIComponent(host))).json();
    $('wcfg-ssh-user').value=sc.ssh_user||'root';
    $('wcfg-ssh-port').value=sc.ssh_port||22;
    $('wcfg-ssh-auth').value=sc.auth_method||'key';
    $('wcfg-ssh-key').value=sc.key_path||'~/.ssh/id_ed25519';
    if($('wcfg-ssh-details')) $('wcfg-ssh-details').open = !(sc && sc.exists);
  } catch(_){ if($('wcfg-ssh-details')) $('wcfg-ssh-details').open=true; }
  $('wcfg-ssh-pw-l').style.display=$('wcfg-ssh-auth').value==='password'?'':'none';
  $('wcfg-ssh-key-l').style.display=$('wcfg-ssh-auth').value==='key'?'':'none';
  openModal('modal-wcfg');
}

async function applyWorkerConfig() {
  if(!_wcfgSid) return;
  const body={
    concurrency: parseInt($('wcfg-conc').value,10),
    transcribe_enabled: $('wcfg-trans').value==='1',
    model: ($('wcfg-model').value||'').trim() || undefined,
    vad: $('wcfg-vad').value==='1',
    beam_size: parseInt($('wcfg-beam').value,10),
    ssh_user: $('wcfg-ssh-user').value.trim(),
    ssh_port: parseInt($('wcfg-ssh-port').value,10),
    auth_method: $('wcfg-ssh-auth').value,
    ssh_password: $('wcfg-ssh-pw').value,
    ssh_key_path: $('wcfg-ssh-key').value.trim(),
  };
  const out=$('wcfg-out'), btn=$('wcfg-apply');
  out.style.display=''; out.textContent=''; btn.disabled=true; btn.textContent='Applying…';
  try {
    const resp=await fetch(`/api/storage/${_wcfgSid}/worker-config`,{method:'POST',credentials:'include',
      headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    if(!resp.ok){ out.textContent+=`[ERROR] HTTP ${resp.status}\n`+await resp.text(); }
    else { const rd=resp.body.getReader(), dec=new TextDecoder();
      while(true){const {value,done}=await rd.read(); if(done)break;
        out.textContent+=dec.decode(value,{stream:true}); out.scrollTop=out.scrollHeight;} }
  } catch(e){ out.textContent+='\n[ERROR] '+e.message; }
  finally { btn.disabled=false; btn.textContent='Apply & restart'; setTimeout(loadStorage,1500); }
}

async function saveStorage() {
  const url = $('st-url').value.trim();
  const token = $('st-token').value.trim();
  const s = $('st-status');
  if (!url || !token) {
    s.style.color = '#f99'; s.textContent = 'URL and token are both required.'; return;
  }
  s.style.color = '#9aa'; s.textContent = 'Adding and testing connection…';
  const sshHost = ($('st-ssh-host')?.value||'').trim();
  const sshPort = parseInt($('st-ssh-port')?.value||'',10);
  let q = `/api/storage?url=${encodeURIComponent(url)}&token=${encodeURIComponent(token)}`;
  if (sshHost) q += `&ssh_host=${encodeURIComponent(sshHost)}`;
  if (sshPort) q += `&ssh_port=${sshPort}`;
  const r = await api(q, {method:'POST'});
  if (r && r.ok) {
    const d = await r.json();
    s.style.color = d.reachable ? '#6ee7a7' : '#fc6';
    s.textContent = d.reachable
      ? 'Added — storage server is reachable.'
      : 'Added, but the server is not responding yet (check it\u2019s running).';
    $('st-url').value = ''; $('st-token').value = '';
    loadStorage();
  } else {
    s.style.color = '#f99'; s.textContent = 'Add failed.';
  }
}

async function removeStorage(id, label) {
  if (!confirm(`Remove storage server "${label}"? Recordings already on it stay `
    + 'there — this only stops the control plane from using it.')) return;
  const r = await api('/api/storage/' + encodeURIComponent(id), {method:'DELETE'});
  if (r && r.ok) loadStorage();
}

// ── Deploy ────────────────────────────────────────────────────────
let _currentDeployType = 'recorder';

function setDeployType(type) {
  _currentDeployType = type;
  const isRec  = type === 'recorder';
  const isSto  = type === 'storage';
  const isPush = type === 'push';
  $('d-recorder-section') && ($('d-recorder-section').style.display = isRec ? '' : 'none');
  $('d-storage-section')  && ($('d-storage-section').style.display  = isSto ? '' : 'none');
  $('d-note-recorder') && ($('d-note-recorder').style.display = isRec ? '' : 'none');
  $('d-note-storage')  && ($('d-note-storage').style.display  = isSto ? '' : 'none');
  $('d-push-section').style.display = isPush ? '' : 'none';
  $('d-ports-section') && ($('d-ports-section').style.display = isPush ? 'none' : '');
  updatePortSummary();
  ['recorder','storage','push'].forEach(t => {
    const el = $('dtype-'+t); if (!el) return;
    el.style.background = type===t ? '#2d2d2d' : '';
    el.style.color      = type===t ? '#fff'    : '';
  });
  $('d-btn').textContent = isPush ? 'Push update' : 'Deploy';
  $('d-result').style.display = 'none';
  $('d-out').textContent = '';
  if (isPush) loadPushTargets();
}

function _deployDefaultPort() { return _currentDeployType === 'storage' ? 8090 : 8000; }

// Live, plain-English summary of what will be deployed and where the control
// plane will reach it. Connect address follows the listen port by default.
function updatePortSummary() {
  if (_currentDeployType === 'push') return;
  const isSto = _currentDeployType === 'storage';
  const svc = isSto ? 'Storage server' : 'Recorder backend';
  const unit = isSto ? 'tt-transcription' : 'tt-backend';
  const def = _deployDefaultPort();
  const listen = parseInt($('d-listen-port').value, 10) || def;
  const host = ($('d-host').value || '').trim() || 'this server';
  const pubHost = ($('d-pub-host').value || '').trim() || (($('d-host').value || '').trim() || 'this server');
  const pubPort = parseInt($('d-pub-port').value, 10) || listen;
  const hint = $('d-listen-hint'); if (hint) hint.textContent = ` — default ${def}`;
  const title = $('d-ports-title'); if (title) title.textContent = `${svc} (${unit}) — port`;
  const s = $('d-port-summary');
  if (s) s.innerHTML =
    `• Binds <span class="mono">0.0.0.0:${listen}</span> on <span class="mono">${esc(host)}</span><br>`
    + `• Control plane will connect to <span class="mono">http://${esc(pubHost)}:${pubPort}</span>`;
}

// ── Push update ───────────────────────────────────────────────────
function onPushKindSelect() { loadPushTargets(); }

function onPushTargetSelect(sel) {
  const opt = sel.options[sel.selectedIndex];
  if (opt && opt.dataset.host) $('d-host').value = opt.dataset.host;
}

async function loadPushTargets() {
  const kind = $('d-push-kind').value;
  const sel = $('d-push-target');
  sel.innerHTML = '<option value="">Loading…</option>';
  const note = $('d-push-note');

  if (kind === 'recorder' || kind === 'colocated') {
    note.textContent = kind === 'colocated'
      ? 'Pushes watcher.py, app.py, chat_recorder.py and transcription_worker.py, restarts both services.'
      : 'Pushes watcher.py, app.py and chat_recorder.py, restarts tt-backend.';
    const r = await api('/api/backends'); if (!r) return;
    const bs = await r.json();
    // For colocated, only show backends whose host also has a storage server
    let storageHosts = new Set();
    if (kind === 'colocated') {
      const sr = await api('/api/storage');
      if (sr && sr.ok) (await sr.json()).forEach(s => { try { storageHosts.add(new URL(s.url).hostname); } catch(_){} });
    }
    const list = bs.filter(b => {
      if (kind !== 'colocated') return true;
      try { return storageHosts.has(new URL(b.url).hostname); } catch(_) { return false; }
    });
    if (!list.length) {
      sel.innerHTML = `<option value="">${kind==='colocated'?'No colocated servers found':'No backends registered'}</option>`;
      return;
    }
    sel.innerHTML = list.map(b => {
      let host=''; try { host=new URL(b.url).hostname; } catch(_){}
      return `<option value="${b.id}" data-host="${esc(host)}">${esc(b.backend_id)} (${esc(host)})</option>`;
    }).join('');
  } else { // storage
    note.textContent = 'Pushes transcription_worker.py, restarts tt-transcription.';
    const r = await api('/api/storage'); if (!r) return;
    const ss = await r.json();
    if (!ss.length) { sel.innerHTML = '<option value="">No storage servers</option>'; return; }
    sel.innerHTML = ss.map(s => {
      let host=''; try { host=new URL(s.url).hostname; } catch(_){}
      return `<option value="${s.id}" data-host="${esc(host)}">${esc(s.label||host)} (${esc(host)})</option>`;
    }).join('');
  }
  onPushTargetSelect(sel);
}

function toggleAuth() {
  const m = $('d-auth').value;
  $('d-key-lbl').style.display = m === 'key' ? '' : 'none';
  $('d-pw-lbl').style.display  = m === 'password' ? '' : 'none';
}
function togglePw() {
  const i = $('d-pw'); i.type = i.type === 'password' ? 'text' : 'password';
}

async function runDeploy() {
  const out = $('d-out'), btn = $('d-btn'), res = $('d-result');
  out.textContent = ''; res.style.display = 'none';
  const isPush = _currentDeployType === 'push';
  btn.disabled = true; btn.textContent = isPush ? 'Pushing…' : 'Deploying...';

  if (isPush) {
    const targetId = $('d-push-target').value;
    if (!targetId) { out.textContent = '[ERROR] Select a server\n'; btn.disabled=false; btn.textContent='Push update'; return; }
    const body = {
      update_type: $('d-push-kind').value, target_id: targetId,
      ssh_port: parseInt($('d-port').value,10), ssh_user: $('d-user').value.trim(),
      auth_method: $('d-auth').value, ssh_password: $('d-pw').value, ssh_key_path: $('d-key').value.trim(),
    };
    try {
      const resp = await fetch('/api/push-update', {method:'POST', credentials:'include',
        headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)});
      if (!resp.ok) { out.textContent += `[ERROR] HTTP ${resp.status}\n`+await resp.text(); return; }
      const reader = resp.body.getReader(), dec = new TextDecoder();
      while(true){ const {value,done}=await reader.read(); if(done)break;
        out.textContent += dec.decode(value,{stream:true}); out.scrollTop=out.scrollHeight; }
      if (out.textContent.includes('Update complete')) {
        showResult('Update pushed', 'Service restarted and healthy.');
        $('d-result-status').style.color = '#6ee7a7';
      }
    } catch(e){ out.textContent += '\n[ERROR] '+e.message+'\n'; }
    finally { btn.disabled=false; btn.textContent='Push update'; }
    return;
  }


  const host = $('d-host').value.trim();
  if (!host) {
    out.textContent = '[ERROR] Host is required\n';
    btn.disabled = false; btn.textContent = 'Deploy'; return;
  }

  const listenPort = parseInt($('d-listen-port').value, 10) || _deployDefaultPort();
  const body = {
    deploy_type:  _currentDeployType,
    host,
    ssh_port:     parseInt($('d-port').value, 10),
    ssh_user:     $('d-user').value.trim(),
    auth_method:  $('d-auth').value,
    ssh_password: $('d-pw').value,
    ssh_key_path: $('d-key').value.trim(),
    service_port: listenPort,
    // unused fields kept for API compatibility
    backend_id: '', region: '', bind_address: '0.0.0.0', whisper_model: 'base',
  };

  try {
    const resp = await fetch('/api/deploy', {
      method: 'POST', credentials: 'include',
      headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body),
    });
    if (!resp.ok) { out.textContent += `[ERROR] HTTP ${resp.status}\n` + await resp.text(); return; }

    const reader = resp.body.getReader(), dec = new TextDecoder();
    let buf = '';
    while (true) {
      const {value, done} = await reader.read(); if (done) break;
      const chunk = dec.decode(value, {stream: true});
      buf += chunk; out.textContent += chunk; out.scrollTop = out.scrollHeight;
    }

    const pubHost = ($('d-pub-host').value||'').trim() || host;
    const pubPort = parseInt($('d-pub-port').value,10) || listenPort;
    const regUrl = `http://${pubHost}:${pubPort}`;
    const sshPort = parseInt($('d-port').value,10);
    if (_currentDeployType === 'storage') {
      const m = buf.match(/WHISPER_AUTH_TOKEN:\s*([0-9a-f]{64})/);
      if (m) {
        showResult('Storage server deployed', `Registering ${regUrl} ...`);
        await autoRegisterStorage(regUrl, m[1], host, sshPort);
      }
    } else {
      const m = buf.match(/AUTH_TOKEN:\s*([0-9a-f]{64})/);
      const bid = buf.match(/Backend ID:\s*(\S+)/);
      if (m) {
        const label = bid ? bid[1] : host;
        showResult(`Recorder backend deployed (${label})`, `Registering ${regUrl} ...`);
        await autoRegisterBackend(regUrl, m[1], host, sshPort);
      }
    }
  } catch(e) {
    out.textContent += '\n[ERROR] ' + e.message + '\n';
  } finally {
    btn.disabled = false; btn.textContent = 'Deploy';
  }
}

function showResult(title, statusText) {
  $('d-result-title').textContent = title;
  $('d-result-status').textContent = statusText;
  $('d-result-status').style.color = '#9aa';
  $('d-result').style.display = 'block';
  $('d-result').scrollIntoView({behavior:'smooth', block:'nearest'});
}

async function autoRegisterBackend(url, token, sshHost, sshPort) {
  for (let attempt = 1; attempt <= 5; attempt++) {
    $('d-result-status').textContent = `Registering backend... (attempt ${attempt}/5)`;
    const r = await api('/api/backends', {
      method: 'POST',
      body: JSON.stringify({url, auth_token: token, ssh_host: sshHost||null, ssh_port: sshPort||null}),
    });
    if (r && r.ok) {
      $('d-result-status').style.color = '#6ee7a7';
      $('d-result-status').textContent = 'Registered. Check the Backends tab.';
      loadBackends();
      return;
    }
    if (attempt < 5) await new Promise(r => setTimeout(r, 3000));
  }
  $('d-result-status').style.color = '#f99';
  $('d-result-status').textContent = 'Auto-register failed — add manually in Backends tab.';
}

async function autoRegisterStorage(url, token, sshHost, sshPort) {
  $('d-result-status').textContent = 'Registering transcript worker...';
  try {
    let q = `/api/storage?url=${encodeURIComponent(url)}&token=${encodeURIComponent(token)}`;
    if (sshHost) q += `&ssh_host=${encodeURIComponent(sshHost)}`;
    if (sshPort) q += `&ssh_port=${sshPort}`;
    const r = await api(q, {method: 'POST'});
    if (r && r.ok) {
      const data = await r.json();
      $('d-result-status').style.color = '#6ee7a7';
      $('d-result-status').textContent = data.reachable
        ? 'Registered. Files tab now shows transcript and storage status.'
        : 'Registered (service still starting — check in 30s).';
    } else {
      $('d-result-status').style.color = '#f99';
      $('d-result-status').textContent = 'Registration failed — restart control plane.';
    }
  } catch(e) {
    $('d-result-status').style.color = '#f99';
    $('d-result-status').textContent = 'Error: ' + e.message;
  }
}

// ── modals close on backdrop click or Escape ───────────────────────
// ── modals close on backdrop click or Escape ───────────────────────
document.querySelectorAll('.modal-bg').forEach(m=>{
  m.addEventListener('click',e=>{if(e.target===m)m.classList.remove('show');});
});
document.addEventListener('keydown',e=>{
  if(e.key==='Escape')
    document.querySelectorAll('.modal-bg.show').forEach(m=>m.classList.remove('show'));
});

// ── version banner + update-all ───────────────────────────────────
async function loadVersionStatus() {
  const r = await api('/api/version-status');
  if (!r || !r.ok) return;
  const v = await r.json();
  const banner = $('version-banner');
  const stale = (v.outdated||[]).concat(v.unknown||[]);
  if (!stale.length) { banner.style.display='none'; return; }
  const names = stale.map(n => `${esc(n.name)} (${n.build?esc(n.build):'old'})`).join(', ');
  banner.style.display = '';
  banner.innerHTML = `⚠ <strong>${stale.length} node(s) running old code</strong>
    vs control plane <code>${esc(v.expected)}</code>: ${names}.
    Mismatched versions cause the move/transfer errors you've seen.
    <button class="primary" style="margin-left:8px;font-size:12px;padding:3px 10px;"
      onclick="runUpdateAll()">Update all now</button>
    <span class="dim" style="font-size:11px;">(uses saved SSH credentials per host)</span>`;
}

async function runUpdateAll() {
  if (!confirm('Push the latest code to every registered node using saved SSH credentials, and restart their services?')) return;
  let win = $('updateall-out');
  if (!win) {
    win = document.createElement('pre');
    win.id = 'updateall-out';
    win.style.cssText = 'background:#0e0e0e;border:1px solid #2a2a2a;border-radius:6px;padding:10px;margin-top:10px;max-height:320px;overflow:auto;white-space:pre-wrap;font-size:11px;';
    $('version-banner').appendChild(win);
  }
  win.textContent = '';
  try {
    const resp = await fetch('/api/update-all', {method:'POST', credentials:'include'});
    const reader = resp.body.getReader(), dec = new TextDecoder();
    while (true) { const {value,done} = await reader.read(); if (done) break;
      win.textContent += dec.decode(value,{stream:true}); win.scrollTop = win.scrollHeight; }
  } catch(e) { win.textContent += '\n[ERROR] '+e.message; }
  loadVersionStatus(); loadStorage && loadStorage();
}

// ── auto-refresh every 5s for active tab ──────────────────────────
loadServers();
loadVersionStatus();
setInterval(()=>{
  const tab=document.querySelector('nav button.active')?.dataset?.tab;
  if(tab==='servers')  loadServers();
  if(tab==='watchers') loadWatchers();
  if(tab==='files')    loadFiles();
  if(tab==='transcription') loadTranscription();
  if(tab==='updates' && !window._updateRunning) loadUpdates();
}, 5000);
setInterval(loadVersionStatus, 30000);

let _purgePk = null;
function openPurge(pk, label) {
  _purgePk = pk;
  $('purge-title').textContent = 'Clean up — ' + label;
  $('purge-out').style.display = 'none';
  $('purge-out').textContent = '';
  $('purge-btn').disabled = false;
  $('purge-btn').textContent = '🧹 Delete confirmed copies now';
  openModal('modal-purge');
}
async function runPurge() {
  if (!_purgePk) return;
  $('purge-btn').disabled = true;
  $('purge-btn').textContent = 'Running…';
  const out = $('purge-out');
  out.style.display = 'block'; out.textContent = '';
  try {
    const res = await fetch('/api/backends/' + _purgePk + '/purge-confirmed', {method:'POST', credentials: 'include'});
    const reader = res.body.getReader(); const dec = new TextDecoder();
    while (true) {
      const {done, value} = await reader.read(); if (done) break;
      out.textContent += dec.decode(value);
      out.scrollTop = out.scrollHeight;
    }
  } catch(e) { out.textContent += '\nError: ' + e; }
  $('purge-btn').textContent = 'Done';
}
