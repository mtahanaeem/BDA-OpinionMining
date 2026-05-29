/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      colors: {
        magenta: { DEFAULT: '#d91b5c', light: '#fce4ec', dark: '#a31545' },
        cerulean: { DEFAULT: '#38bdf8', light: '#e0f2fe', dark: '#0284c7' },
        tangerine: { DEFAULT: '#f97316', light: '#ffedd5', dark: '#c2410c' },
        slate: { DEFAULT: '#1e293b', light: '#334155', dark: '#0f172a' },
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'Segoe UI', 'Roboto', 'sans-serif'],
        mono: ['ui-monospace', 'Consolas', 'monospace'],
      },
    },
  },
  plugins: [],
}
