import socket
import dns.resolver
import requests
from typing import List, Tuple
import logging

logger = logging.getLogger(__name__)

def check_dns_connectivity(domains: List[str] = None) -> Tuple[bool, List[str]]:
    """
    Check DNS connectivity for specified domains.
    
    Args:
        domains: List of domains to check. If None, uses default list.
        
    Returns:
        Tuple of (is_connected, list_of_failed_domains)
    """
    if domains is None:
        domains = [
            'fc.yahoo.com',
            'query1.finance.yahoo.com',
            'query2.finance.yahoo.com'
        ]
    
    failed_domains = []
    
    for domain in domains:
        try:
            # Try DNS resolution
            answers = dns.resolver.resolve(domain, 'A')
            if not answers:
                failed_domains.append(domain)
                logger.warning(f"DNS resolution failed for {domain}")
                continue
                
            # Try HTTP connection
            ip = answers[0].to_text()
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((ip, 443))
            sock.close()
            
            if result != 0:
                failed_domains.append(domain)
                logger.warning(f"Connection failed for {domain} (IP: {ip})")
                
        except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer, dns.resolver.Timeout) as e:
            failed_domains.append(domain)
            logger.warning(f"DNS resolution error for {domain}: {str(e)}")
        except Exception as e:
            failed_domains.append(domain)
            logger.warning(f"Error checking {domain}: {str(e)}")
    
    return len(failed_domains) == 0, failed_domains

def check_pihole_blocking(domains: List[str] = None) -> Tuple[bool, List[str]]:
    """
    Check if domains are being blocked by PiHole.
    
    Args:
        domains: List of domains to check. If None, uses default list.
        
    Returns:
        Tuple of (is_blocked, list_of_blocked_domains)
    """
    if domains is None:
        domains = [
            'fc.yahoo.com',
            'query1.finance.yahoo.com',
            'query2.finance.yahoo.com'
        ]
    
    blocked_domains = []
    
    for domain in domains:
        try:
            # Try to connect to PiHole API
            response = requests.get('http://pi.hole/admin/api.php?status')
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'enabled':
                    # Check if domain is in blocklist
                    block_response = requests.get(f'http://pi.hole/admin/api.php?getAllQueries&domain={domain}')
                    if block_response.status_code == 200:
                        block_data = block_response.json()
                        if block_data.get('data', []):
                            blocked_domains.append(domain)
                            logger.warning(f"Domain {domain} appears to be blocked by PiHole")
        except requests.RequestException:
            # If we can't connect to PiHole API, assume it's not running
            pass
        except Exception as e:
            logger.warning(f"Error checking PiHole for {domain}: {str(e)}")
    
    return len(blocked_domains) > 0, blocked_domains

def verify_yahoo_finance_connectivity() -> bool:
    """
    Verify connectivity to Yahoo Finance domains.
    
    Returns:
        bool: True if all checks pass, False otherwise
    """
    # Check DNS connectivity
    dns_ok, failed_dns = check_dns_connectivity()
    if not dns_ok:
        logger.error(f"DNS connectivity issues detected for: {', '.join(failed_dns)}")
        return False
    
    # Check PiHole blocking
    pihole_blocked, blocked_domains = check_pihole_blocking()
    if pihole_blocked:
        logger.error(f"Domains appear to be blocked by PiHole: {', '.join(blocked_domains)}")
        return False
    
    return True 