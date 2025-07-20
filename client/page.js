const viewerCount = document.getElementById('count');
const pageHeader = document.getElementById('page-header');

const pathParts = location.pathname.split('/');
const pagePart = pathParts.find(part => part.startsWith('page-'));
const pageNumber = pagePart ? pagePart.split('-')[1] : 'unknown';

pageHeader.textContent = `You are viewing Page ${pageNumber}`;

const wsUrl = `wss://lw-sub.clumsycoder.com/ws/page-${pageNumber}`;
let socket = null;

function connectWebSocket() {
    socket = new WebSocket(wsUrl);

    socket.onopen = () => {
        console.log(`WebSocket connected for page-${pageNumber}`);
    };

    socket.onmessage = (event) => {
        viewerCount.textContent = event.data;
        viewerCount.style.visibility = 'visible';
    };

    socket.onerror = (err) => {
        console.error("WebSocket error:", err);
        viewerCount.textContent = "-1";
        viewerCount.style.visibility = 'visible';
    };

    socket.onclose = (event) => {
        console.warn(`WebSocket closed (code: ${event.code}, reason: ${event.reason})`);
        viewerCount.textContent = "-1";
        viewerCount.style.visibility = 'visible';
        setTimeout(connectWebSocket, 2000);
    };
}

connectWebSocket();
