/** Dashboard JS — interaction handlers, keyboard, polling. */
export const DASHBOARD_JS_EVENTS = `
function toggleMsg(id){if(expMsgs.has(id)){expMsgs.delete(id);render();return}expMsgs.add(id);const t=cur();render();if(t&&!fullMessageBodies.has(id))ensureTeamMessages(t.id).then(render).catch(()=>{})}

function toggleVerbose(){
  verbose=!verbose;
  try{localStorage.setItem('ensemble-verbose',verbose?'1':'0')}catch(e){}
  var btn=document.getElementById('verbose-toggle');
  if(btn){
    btn.textContent='verbose: '+(verbose?'on':'off');
    btn.setAttribute('aria-pressed',verbose?'true':'false');
    btn.className='text-[10px] '+(verbose?'text-blue-400 border-blue-500/40 bg-blue-500/10':'text-txt-500 hover:text-txt-200 border-base-800')+' rounded px-1.5 py-[2px] transition-colors';
  }
  rDrawerActivityUpdate();
}

var fetchActivityGen=0;
async function fetchActivity(sessionId){
  var gen=++fetchActivityGen;
  try{
    var res=await apiFetch('api/session/'+encodeURIComponent(sessionId)+'/activity');
    var data=await res.json();
    if(gen!==fetchActivityGen)return;
    drawerActivity=data.activity||[];
    drawerSession=data.session||null;
    rDrawerActivityUpdate();
  }catch{if(gen!==fetchActivityGen)return;drawerActivity=[];drawerSession=null;rDrawerActivityUpdate()}
}

function applyNavCollapse(){
  const content=document.getElementById('content'),projects=document.getElementById('projects'),rail=document.getElementById('project-rail'),toggle=document.getElementById('nav-toggle'),expand=document.getElementById('nav-expand');
  content.classList.toggle('nav-collapsed',navCollapsed);
  projects.hidden=navCollapsed;
  projects.setAttribute('aria-hidden',String(navCollapsed));
  rail.hidden=!navCollapsed;
  if(toggle)toggle.setAttribute('aria-expanded',String(!navCollapsed));
  expand.setAttribute('aria-expanded',String(!navCollapsed));
  if(document.activeElement===toggle&&navCollapsed)expand.focus();
  if(document.activeElement===expand&&!navCollapsed&&toggle)toggle.focus();
}

function render(){
  rSel();const t=cur();
  rTeamSwitcher(t);
  const empty=document.getElementById('empty'),content=document.getElementById('content');
  if(!t){empty.classList.remove('hidden');empty.classList.add('flex');content.classList.add('hidden');document.getElementById('tl').classList.add('hidden');return}
  empty.classList.add('hidden');empty.classList.remove('flex');content.classList.remove('hidden');
  applyNavCollapse();
  const p=curProject();document.getElementById('crumb').textContent=p?' / '+projectLabel(p)+' / '+t.name:'';
  document.querySelectorAll('.view-link').forEach(el=>el.setAttribute('aria-current',String(el.dataset.view===selectedView)));
  document.getElementById('overview-view').classList.toggle('hidden',selectedView!=='overview');
  document.getElementById('conversation-view').classList.toggle('hidden',selectedView!=='conversations');
  if(selectedView==='overview'){rHealth(t);rSum(t);rAttention(t);rAgents(t);rTasks(t);rActivity(t);rTimeline(t)}else{rHealth(t);rSum(t);document.getElementById('tl').classList.add('hidden');rConversations(t)}
}

function syncLocation(replace){const q=new URLSearchParams();if(selectedView!=='overview')q.set('view',selectedView);if(showArchived)q.set('archived','1');if(selProjectId)q.set('project',selProjectId);if(selId)q.set('team',selId);if(selectedView==='conversations'&&selectedChannel)q.set('channel',selectedChannel);const url=location.pathname+(q.toString()?'?'+q.toString():'');history[replace?'replaceState':'pushState'](null,'',url)}
function selectView(view){selectedView=view==='conversations'?'conversations':'overview';conversationError='';syncLocation(false);render()}
function selectProject(id){selProjectId=id;const p=S?.projects?.find(p=>p.id===id),t=p?visibleProjectTeams(p).sort((a,b)=>b.timeUpdated-a.timeUpdated)[0]:null;if(t)selId=t.id;selectedChannel='broadcast';selCard=-1;syncLocation(false);render()}
function selectTeam(id){selId=id;const t=cur();selProjectId=t?.projectId||selProjectId;const cp=channelParts(selectedChannel),valid=cp.type==='broadcast'||cp.type==='member'&&(t?.members||[]).some(m=>m.name===cp.name)||cp.type==='group'&&(t?.groups||[]).some(g=>g.name===cp.name);if(!valid)selectedChannel='broadcast';selCard=-1;conversationError='';syncLocation(false);render()}
function toggleArchived(){showArchived=!showArchived;if(!showArchived){const selected=S?.teams?.find(t=>t.id===selId);if(selected&&selected.status!=='active'){selId=null;selectedChannel='broadcast'}}render();syncLocation(false)}
function selectConversation(channel){selectedView='conversations';selectedChannel=channel||'broadcast';conversationError='';syncLocation(false);render()}
function loadOlderMessages(){const t=cur();if(!t)return;ensureConversation(t.id,selectedChannel,true).then(()=>render()).catch(err=>{conversationError=err.message;render()})}
async function sendConversationMessage(event){event.preventDefault();if(conversationSending)return;const t=cur(),text=document.getElementById('conversation-text'),cp=channelParts(selectedChannel);if(!t||!text)return;const content=text.value.trim();if(!content){conversationError='Enter a message first.';render();return}conversationSending=true;conversationError='';render();try{const destination=cp.type==='group'?{group:cp.name,content:content}:{to:cp.type==='broadcast'?null:cp.name,content:content};await apiFetch('api/teams/'+encodeURIComponent(t.id)+'/messages',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(destination)});text.value='';conversationPages.delete(conversationKey(t.id,selectedChannel));await ensureConversation(t.id,selectedChannel,false);conversationError='';}catch(err){conversationError=err.message||'Message could not be queued.'}finally{conversationSending=false;render();document.getElementById('conversation-text')?.focus()}}

function conn(ok){
  document.getElementById('cd').className='w-[7px] h-[7px] rounded-full '+(ok?'bg-emerald-500 pulse':'bg-red-500');
  document.getElementById('ct').textContent=ok?D(Date.now()-pollT)+' ago':'reconnecting to dashboard state';
}

async function poll(){try{const previous=cur(),previousLatest=previous?.messages?.[0]?.id||null;S=await(await apiFetch('api/state')).json();const current=cur(),currentLatest=current?.messages?.[0]?.id||null;if(selectedView==='conversations'&&previousLatest!==currentLatest&&current)conversationPages.delete(conversationKey(current.id,selectedChannel));fails=0;pollT=Date.now();conn(true);render()}catch{if(++fails>=3)conn(false)}}

function setBackgroundInert(locked){
  document.querySelectorAll('header,main,#sum,#tl').forEach(function(el){
    el.inert=locked;
    if(locked)el.setAttribute('aria-hidden','true');
    else el.removeAttribute('aria-hidden');
  });
}

function modalOpen(){return document.getElementById('sco').classList.contains('show')||document.getElementById('drawer').classList.contains('open')}

function trapFocus(root,e){
  const nodes=[...root.querySelectorAll('button,[href],input,select,textarea,[tabindex]:not([tabindex="-1"])')].filter(function(el){return !el.disabled&&!el.inert&&el.offsetParent!==null});
  if(!nodes.length){e.preventDefault();root.focus();return}
  const first=nodes[0],last=nodes[nodes.length-1];
  if(e.shiftKey&&document.activeElement===first){e.preventDefault();last.focus();return}
  if(!e.shiftKey&&document.activeElement===last){e.preventDefault();first.focus();return}
}

function openShortcuts(){
  const el=document.getElementById('sco');
  setBackgroundInert(true);
  el.classList.add('show');
  el.setAttribute('aria-hidden','false');
  document.getElementById('sco').focus();
}

function closeShortcuts(){
  const el=document.getElementById('sco');
  el.classList.remove('show');
  el.setAttribute('aria-hidden','true');
  if(!modalOpen())setBackgroundInert(false);
}

// Clock update every second
setInterval(function(){var t=cur();if(t)rClock(t);if(fails<3)conn(true)},1000);

// Poll every 2.5s
setInterval(poll,2500);

document.addEventListener('click',function(e){
  const id=e.target&&e.target.id;
  if(id!=='nav-toggle'&&id!=='nav-expand')return;
  navCollapsed=id==='nav-toggle';
  applyNavCollapse();
});

addEventListener('popstate',function(){const q=new URLSearchParams(location.search);selectedView=q.get('view')==='conversations'?'conversations':'overview';showArchived=q.get('archived')==='1';selProjectId=q.get('project');selId=q.get('team');selectedChannel=q.get('channel')||(q.get('member')?'member:'+q.get('member'):'broadcast');conversationError='';render()});

// Keyboard shortcuts
document.addEventListener('keydown',function(e){
  const shortcutsOpen=document.getElementById('sco').classList.contains('show');
  if(shortcutsOpen&&e.key==='Tab'){trapFocus(document.getElementById('sco'),e);return}
  if(shortcutsOpen&&e.key==='Escape'){e.preventDefault();closeShortcuts();return}
  const drawerOpen=document.getElementById('drawer').classList.contains('open');
  if(drawerOpen&&e.key==='Tab'){trapFocus(document.getElementById('drawer'),e);return}
  if(drawerOpen&&e.key==='Escape'){e.preventDefault();closeDrawer();return}
  if(drawerOpen&&e.key==='?'){e.preventDefault();return}
  if(e.target.tagName==='INPUT'||e.target.tagName==='SELECT'||e.target.tagName==='TEXTAREA')return;
  var t=cur();if(!t)return;
  var mm=[...(t.members||[])].sort((a,b)=>rankAgent(a,b,t));
  if(e.key==='?'){e.preventDefault();shortcutsOpen?closeShortcuts():openShortcuts();return}
  if(e.key==='Escape'){closeDrawer();expMsgs.clear();selCard=-1;closeShortcuts();render();return}
  if(e.key==='j'&&mm.length){e.preventDefault();selCard=Math.min(selCard+1,mm.length-1);render();return}
  if(e.key==='k'&&mm.length){e.preventDefault();selCard=Math.max(selCard-1,0);render();return}
  if(e.key==='v'&&!e.ctrlKey&&!e.metaKey&&!e.altKey){e.preventDefault();toggleVerbose();return}
  if(e.key==='Enter'&&selCard>=0&&selCard<mm.length){e.preventDefault();openDrawer(mm[selCard].name);return}
  if(e.key>='1'&&e.key<='9'){
    var teams=allTeams(),all=[...teams.active,...teams.archived];
    var idx=parseInt(e.key)-1;
    if(idx<all.length){selId=all[idx].id;render()}
  }
});

// Initial poll
poll();

console.log('%c Ensemble Mission Control','font-size:14px;font-weight:bold;color:#22c55e');
`;
