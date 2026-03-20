 /**
 * useTelegram.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Wraps window.Telegram.WebApp with React state and graceful fallbacks.
 * Works correctly both inside Telegram and in a normal browser (dev mode).
 *
 * USAGE
 *   const { tg, user, viewportHeight, isExpanded, haptic } = useTelegram()
 * ─────────────────────────────────────────────────────────────────────────────
 */

import { useState, useEffect, useCallback } from 'react'

/** Safe accessor — returns null if SDK not present */
function getTg() {
  return window?.Telegram?.WebApp ?? null
}

export function useTelegram() {
  const tg = getTg()

  const [viewportHeight, setViewportHeight] = useState(
    tg?.viewportStableHeight ?? window.innerHeight
  )
  const [isExpanded, setIsExpanded] = useState(tg?.isExpanded ?? false)

  // ── Initialise on mount ──────────────────────────────────────────────────
  useEffect(() => {
    const t = getTg()
    if (!t) return

    // Signal the app is ready — hides the native loading spinner
    t.ready()

    // Ask for full viewport height
    t.expand()
    setIsExpanded(t.isExpanded)

    // Keep height in sync when the user swipes or keyboard appears
    const onViewport = () => {
      setViewportHeight(t.viewportStableHeight || window.innerHeight)
      setIsExpanded(t.isExpanded)
    }
    t.onEvent('viewportChanged', onViewport)
    return () => t.offEvent('viewportChanged', onViewport)
  }, [])

  // ── Haptic feedback helpers ──────────────────────────────────────────────
  const haptic = useCallback({
    impact:    (style = 'light') => tg?.HapticFeedback?.impactOccurred(style),
    // style: 'light' | 'medium' | 'heavy' | 'rigid' | 'soft'
    notify:    (type  = 'success') => tg?.HapticFeedback?.notificationOccurred(type),
    // type:  'error' | 'success' | 'warning'
    selection: () => tg?.HapticFeedback?.selectionChanged(),
  }, [tg])

  // ── Back button helpers ──────────────────────────────────────────────────
  const showBackButton = useCallback((onBack) => {
    const t = getTg()
    if (!t) return
    t.BackButton.show()
    t.BackButton.onClick(onBack)
  }, [])

  const hideBackButton = useCallback(() => {
    const t = getTg()
    if (!t) return
    t.BackButton.hide()
  }, [])

  // ── Main button helpers ──────────────────────────────────────────────────
  const showMainButton = useCallback(({ text, color, onClick }) => {
    const t = getTg()
    if (!t) return
    t.MainButton.setText(text)
    if (color) t.MainButton.color = color
    t.MainButton.onClick(onClick)
    t.MainButton.show()
  }, [])

  const hideMainButton = useCallback(() => {
    const t = getTg()
    if (!t) return
    t.MainButton.hide()
  }, [])

  // ── Derived ──────────────────────────────────────────────────────────────
  const user        = tg?.initDataUnsafe?.user ?? null
  const colorScheme = tg?.colorScheme ?? 'dark'   // 'dark' | 'light'
  const platform    = tg?.platform   ?? 'unknown' // 'ios' | 'android' | 'tdesktop' | ...

  // Safe-area insets (iOS notch / home indicator)
  // Telegram exposes these as CSS env() vars — read them here for JS usage too
  const safeAreaTop    = parseInt(getComputedStyle(document.documentElement)
    .getPropertyValue('--tg-safe-area-inset-top')    || '0')
  const safeAreaBottom = parseInt(getComputedStyle(document.documentElement)
    .getPropertyValue('--tg-safe-area-inset-bottom') || '0')

  return {
    tg,
    user,
    viewportHeight,
    isExpanded,
    colorScheme,
    platform,
    safeAreaTop,
    safeAreaBottom,
    haptic,
    showBackButton,
    hideBackButton,
    showMainButton,
    hideMainButton,
    /** true when running inside Telegram */
    isTelegram: !!tg,
  }
}
