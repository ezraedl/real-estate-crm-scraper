import random
import asyncio
import httpx
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
from config import settings

@dataclass
class ProxyConfig:
    host: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None
    is_active: bool = True
    last_used: Optional[float] = None
    success_count: int = 0
    failure_count: int = 0

class ProxyManager:
    def __init__(self):
        self.proxies: List[ProxyConfig] = []
        self.current_proxy_index = 0
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:120.0) Gecko/20100101 Firefox/120.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0"
        ]
        self.headers_templates = [
            {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            },
            {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Upgrade-Insecure-Requests": "1",
            }
        ]
    
    async def initialize_dataimpulse_proxies(self, login: str, password: str = None, endpoint: str = None):
        """Initialize DataImpulse proxies using their direct proxy format"""
        try:
            # Use settings if not provided
            if not password:
                password = settings.DATAIMPULSE_PASSWORD
            if not endpoint:
                endpoint = settings.DATAIMPULSE_ENDPOINT
            
            # DataImpulse doesn't provide an API to fetch proxy lists
            # Instead, they provide direct proxy connections with targeting parameters
            # Based on docs.dataimpulse.com, we create proxy configs with their format
            
            self.proxies = []
            
            # Parse the endpoint to get host and port
            # Remove protocol prefix if present
            if endpoint.startswith('http://'):
                endpoint = endpoint[7:]
            elif endpoint.startswith('https://'):
                endpoint = endpoint[8:]
            
            if ':' in endpoint:
                # Handle cases like "gw.dataimpulse.com:823"
                parts = endpoint.split(':')
                host = parts[0]
                port = int(parts[1])
            else:
                host = endpoint
                port = 823  # Default DataImpulse port
            
            # Create multiple proxy configurations with different targeting
            # This simulates having multiple proxies by using different targeting parameters
            
            # 1. Default proxy (US targeting)
            proxy_us = ProxyConfig(
                host=host,
                port=port,
                username=f"{login}__cr.us",
                password=password
            )
            self.proxies.append(proxy_us)
            
            # 2. Indianapolis specific proxy
            proxy_indy = ProxyConfig(
                host=host,
                port=port,
                username=f"{login}__cr.us;city.indianapolis",
                password=password
            )
            self.proxies.append(proxy_indy)
            
            # 3. Anonymous proxy
            proxy_anon = ProxyConfig(
                host=host,
                port=port,
                username=f"{login}__anonymous",
                password=password
            )
            self.proxies.append(proxy_anon)
            
            # 4. Another US proxy with different session
            proxy_us2 = ProxyConfig(
                host=host,
                port=port,
                username=f"{login}__cr.us;session.2",
                password=password
            )
            self.proxies.append(proxy_us2)
            
            print(f"Initialized {len(self.proxies)} DataImpulse proxy configurations")
            print(f"Using DataImpulse host: {host}:{port}")
            print(f"Proxy targeting: US, Indianapolis, Anonymous, US+Session2")
            return True
                    
        except Exception as e:
            print(f"Error initializing DataImpulse proxies: {e}")
            return False
    
    def add_proxy(self, host: str, port: int, username: Optional[str] = None, password: Optional[str] = None):
        """Add a single proxy"""
        proxy = ProxyConfig(host=host, port=port, username=username, password=password)
        self.proxies.append(proxy)
    
    def get_next_proxy(self) -> Optional[ProxyConfig]:
        """Get the next available proxy using round-robin"""
        if not self.proxies:
            return None
        
        active_proxies = [p for p in self.proxies if p.is_active]
        if not active_proxies:
            # Reset all proxies if none are active
            for proxy in self.proxies:
                proxy.is_active = True
            active_proxies = self.proxies
        
        if not active_proxies:
            return None
        
        # Round-robin selection
        proxy = active_proxies[self.current_proxy_index % len(active_proxies)]
        self.current_proxy_index += 1
        
        return proxy
    
    def get_random_proxy(self) -> Optional[ProxyConfig]:
        """Get a random available proxy"""
        active_proxies = [p for p in self.proxies if p.is_active]
        if not active_proxies:
            return None
        
        return random.choice(active_proxies)
    
    def get_random_user_agent(self) -> str:
        """Get a random user agent"""
        return random.choice(self.user_agents)
    
    def get_random_headers(self) -> Dict[str, str]:
        """Get random headers"""
        headers = random.choice(self.headers_templates).copy()
        headers["User-Agent"] = self.get_random_user_agent()
        return headers
    
    def mark_proxy_success(self, proxy: ProxyConfig):
        """Mark proxy as successful"""
        proxy.success_count += 1
        proxy.last_used = asyncio.get_event_loop().time()
    
    def mark_proxy_failure(self, proxy: ProxyConfig, max_failures: int = 5):
        """Mark proxy as failed and potentially deactivate"""
        proxy.failure_count += 1
        proxy.last_used = asyncio.get_event_loop().time()
        
        # Deactivate if too many failures
        if proxy.failure_count >= max_failures:
            proxy.is_active = False
            print(f"Proxy {proxy.host}:{proxy.port} deactivated due to failures")
    
    def get_proxy_stats(self) -> Dict[str, Any]:
        """Get proxy statistics"""
        total_proxies = len(self.proxies)
        active_proxies = len([p for p in self.proxies if p.is_active])
        
        total_success = sum(p.success_count for p in self.proxies)
        total_failures = sum(p.failure_count for p in self.proxies)
        
        return {
            "total_proxies": total_proxies,
            "active_proxies": active_proxies,
            "total_success": total_success,
            "total_failures": total_failures,
            "success_rate": total_success / (total_success + total_failures) if (total_success + total_failures) > 0 else 0
        }
    
    def reset_proxy_stats(self):
        """Reset all proxy statistics"""
        for proxy in self.proxies:
            proxy.success_count = 0
            proxy.failure_count = 0
            proxy.is_active = True

# Global proxy manager instance
proxy_manager = ProxyManager()
