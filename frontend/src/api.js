import axios from 'axios';

// 기본: 현재 페이지 오리진, 필요 시 VITE_API_BASE로 덮어쓰기
const baseURL = import.meta.env.VITE_API_BASE || window.location.origin;
const api = axios.create({ baseURL });

export const fetchUniverse = () => api.get('/universe').then(r => r.data);
export const fetchPrices = (code, days = 60) => api.get('/prices', { params: { code, days } }).then(r => r.data);
export const fetchSignals = () => api.get('/signals').then(r => r.data);
