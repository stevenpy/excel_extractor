(function(){const r=document.createElement("link").relList;if(r&&r.supports&&r.supports("modulepreload"))return;for(const e of document.querySelectorAll('link[rel="modulepreload"]'))a(e);new MutationObserver(e=>{for(const t of e)if(t.type==="childList")for(const s of t.addedNodes)s.tagName==="LINK"&&s.rel==="modulepreload"&&a(s)}).observe(document,{childList:!0,subtree:!0});function n(e){const t={};return e.integrity&&(t.integrity=e.integrity),e.referrerPolicy&&(t.referrerPolicy=e.referrerPolicy),e.crossOrigin==="use-credentials"?t.credentials="include":e.crossOrigin==="anonymous"?t.credentials="omit":t.credentials="same-origin",t}function a(e){if(e.ep)return;e.ep=!0;const t=n(e);fetch(e.href,t)}})();const i=[{href:"/",label:"Accueil"},{href:"/tarifs.html",label:"Tarifs"},{href:"/contact.html",label:"Contact"}];function c(o){const r=window.location.pathname,n=r==="/"||r===""||r.endsWith("index.html");o.innerHTML=`
    <header class="sticky top-0 z-50 border-b border-[#e6ebf3] bg-white/95 backdrop-blur-md">
      <div class="mx-auto flex h-16 max-w-[83rem] items-center justify-between px-4 sm:px-6 lg:px-8">
        <a href="/" class="flex shrink-0 items-center gap-2">
          <img src="/logo.svg" alt="DevisPilot" class="h-9 w-auto max-w-[200px] object-left object-contain sm:h-10 sm:max-w-[220px]" />
        </a>
        <nav class="hidden items-center gap-10 md:flex" aria-label="Principal">
          ${i.map(({href:t,label:s})=>{const l=t==="/"?n:r.endsWith(t.replace(/^\//,""));return`<a href="${t}" class="text-[0.95rem] font-medium transition-colors ${l?"text-[#121627]":"text-[#4f5875] hover:text-[#121627]"}">${s}</a>`}).join("")}
        </nav>
        <div class="flex items-center gap-4"><a href="/tarifs.html" class="hidden text-[0.95rem] font-semibold text-blurple sm:inline hover:text-[#352daa]"></a>
          
          <a href="/contact.html" class="btn-primary !py-2.5 !text-sm">Réserver une démo</a>
          <button
            type="button"
            class="inline-flex items-center justify-center rounded-lg border border-[#e6ebf3] p-2 text-[#121627] md:hidden"
            aria-label="Menu"
            data-nav-toggle
          >
            <svg class="h-5 w-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h16"/></svg>
          </button>
        </div>
      </div>
      <div class="hidden border-t border-[#e6ebf3] bg-white px-4 py-4 md:hidden" data-nav-panel>
        <nav class="flex flex-col gap-4" aria-label="Mobile">
          ${i.map(({href:t,label:s})=>`<a href="${t}" class="text-sm font-medium text-[#121627]">${s}</a>`).join("")}
          <a href="/contact.html" class="btn-primary mt-2 text-center text-sm">Réserver une démo</a>
        </nav>
      </div>
    </header>
  `;const a=o.querySelector("[data-nav-toggle]"),e=o.querySelector("[data-nav-panel]");a==null||a.addEventListener("click",()=>{e==null||e.classList.toggle("hidden")})}export{c as m};
