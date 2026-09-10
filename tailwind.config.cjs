/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./src/dashboard-*.ts"],
  theme: {
    extend: {
      colors: {
        base: {
          950: "#0c0e14",
          900: "#141822",
          850: "#1a1f2e",
          800: "#1e2433",
          700: "#2a3144",
          600: "#3a4358",
        },
        txt: {
          100: "#e2e8f0",
          200: "#c1c9d9",
          300: "#aab4c6",
          400: "#8a96aa",
          500: "#7b879b",
        },
      },
      fontFamily: {
        sans: ["system-ui", "sans-serif"],
        mono: ["ui-monospace", "monospace"],
      },
    },
  },
}
