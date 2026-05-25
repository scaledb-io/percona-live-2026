import { defineShortcutsSetup } from '@slidev/types'

export default defineShortcutsSetup((nav, base) => [
  ...base,
  {
    key: 'd',
    fn: () => {
      const html = document.documentElement
      const next = !html.classList.contains('dark')
      html.classList.toggle('dark', next)
      html.classList.toggle('light', !next)
      try { localStorage.setItem('slidev-color-schema', next ? 'dark' : 'light') } catch {}
    },
    autoRepeat: false,
  },
])
