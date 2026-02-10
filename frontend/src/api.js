import axios from 'axios';

// Vite 프록시 또는 Nginx 설정에 맞춰 /api 접두사 사용 여부 결정
// 현재 server.py가 직접 루트에서 제공하므로 baseURL을 /bnf로 설정 (Nginx 프록시용)
const baseURL = import.meta.env.VITE_API_BASE || '/bnf';
const api = axios.create({ baseURL });

export const fetchUniverse = () => api.get('/universe').then(r => r.data);
export const fetchPrices = (code, days = 60) => api.get('/prices', { params: { code, days } }).then(r => r.data);
export const fetchSignals = () => api.get('/signals').then(r => r.data);
export const fetchStatus = () => api.get('/status').then(r => r.data);
export const fetchEngines = () => api.get('/engines').then(r => r.data);
export const fetchOrders = () => api.get('/orders').then(r => r.data);
export const fetchPositions = () => api.get('/positions').then(r => r.data);
export const fetchStrategy = () => api.get('/strategy').then(r => r.data);
export const fetchJobs = () => api.get('/jobs').then(r => r.data);
export const triggerExport = () => api.post('/export').then(r => r.data);
