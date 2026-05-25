<template>
  <footer
    v-if="$slidev.nav.currentPage > 1 && $slidev.nav.currentPage < $slidev.nav.total"
    class="deck-footer"
  >
    <div class="deck-footer-left">Streaming MySQL Changes to ClickHouse · Javier Zon</div>
    <div class="deck-footer-right">{{ $slidev.nav.currentPage }} / {{ $slidev.nav.total }}</div>
  </footer>
</template>
