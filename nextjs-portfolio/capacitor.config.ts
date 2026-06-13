import type { CapacitorConfig } from '@capacitor/cli';

// The APK is a thin full-screen shell that loads the live Next.js app hosted on
// Vercel (server-side API routes hold the token). This keeps the phone app 100%
// identical to the web app and working with the PC off.
const config: CapacitorConfig = {
  appId: 'com.ouriel.portfolio_os',
  appName: 'Portfolio OS',
  webDir: 'out',
  server: {
    url: 'https://nextjs-portfolio-ouriel-s-projects1.vercel.app',
    cleartext: false,
  },
};

export default config;
