import meraki.aio
import config
import asyncio
import pandas as pd
from tabulate import tabulate
from datetime import datetime
from datetime import timedelta
import matplotlib.pyplot as plt

net_id = config.net_id

# Instantiate async Meraki API client
aiomeraki = meraki.aio.AsyncDashboardAPI(
    config.API_KEY,
    base_url="https://api.meraki.com/api/v1",
    log_file_prefix=__file__[:-3],
    print_console=False,
    maximum_retries=config.max_retries,
    maximum_concurrent_requests=config.max_requests,
)

# Instantiate synchronous Meraki API client
dashboard = meraki.DashboardAPI(
    config.API_KEY,
    base_url="https://api.meraki.com/api/v1",
    log_file_prefix=__file__[:-3],
    print_console=config.console_logging,
)

# Fetch config parameters
year = config.year
month = config.month
day = config.day
t0_dt = datetime(year, month, day, 0, 0, 0)  # Start time
t0_str = datetime.strftime(t0_dt, '%Y-%m-%dT%H:%M:%S')
num_days = config.num_days  # Number of days to look forward
num_seconds = int(num_days * 24 * 60 * 60)
step = config.step

date_list = [t0_dt + timedelta(seconds=x) for x in range(0, num_seconds, step)]

tag = config.ap_tag
band = config.band
ssid = config.ssid
filters = {}
filters_cc = {}
if tag != "":
    filters['apTag'] = tag
    filters_cc['apTag'] = tag
if band != "":
    filters['band'] = band
if ssid != "":
    filters['ssid'] = ssid
    filters_cc['ssid'] = ssid

conn_stats_full = []
client_counts_full = []
latency_stats_full = []
signal_quality_full = []
channel_utilization_full = []


async def gather_latency_stats(networkId, t0, t1, filters):
    """
    Fetch network latency stats with async
    :param networkId: network ID to use
    :param t0: Start time of interval
    :param t1: End time of interval
    :param filters: Optional filters for band, ssid and apTag
    :return: "latency", latency stats: label to differentiate metric, metric
    """
    latency_stats = await aiomeraki.wireless.getNetworkWirelessLatencyStats(networkId=networkId,
                                                                            t0=t0,
                                                                            t1=t1,
                                                                            **filters)
    # Get average stats only
    if latency_stats != None:
        latency_stats['startTs'] = t0
        latency_stats['endTs'] = t1
        latency_stats['backgroundTrafficAvg'] = latency_stats['backgroundTraffic']['avg']
        latency_stats['bestEffortTrafficAvg'] = latency_stats['bestEffortTraffic']['avg']
        latency_stats['videoTrafficAvg'] = latency_stats['videoTraffic']['avg']
        latency_stats['voiceTrafficAvg'] = latency_stats['voiceTraffic']['avg']
    return "latency", latency_stats


async def gather_conn_stats(networkId, t0, t1, filters):
    """
    Fetch connection statistics with async
    :param networkId: network ID to fetch
    :param t0: Start time of interval
    :param t1: End time of interval
    :param filters: Optional filters for ssid, band and apTag
    :return: "conn", conn_stats_global: label to differentiate metric, metric
    """
    conn_stats = await aiomeraki.wireless.getNetworkWirelessConnectionStats(networkId=networkId,
                                                                            t0=t0,
                                                                            t1=t1,
                                                                            **filters)
    conn_stats_global = {"startTs": t0,
                         "endTs": t1,
                         "connStats": conn_stats}
    return "conn", conn_stats_global


async def gather_wireless_stats(aiomeraki, net_id, date_list, filters):
    """
    Get latency and connection stats for the network on many time intervals separated by step
    :param aiomeraki: async library client
    :param net_id: Network ID to fetch
    :param date_list: List of time intervals
    :param filters: Optional ssid, band and apTag filters
    :return: latency_stats_full, conn_stats_full : Full latency and connectivity stats for the networ in the specified time period
    """
    # Build list of tasks to fetch
    get_tasks = []
    for i in range(len(date_list)):
        if i < len(date_list) - 1:
            date_str_t0 = datetime.strftime(date_list[i], '%Y-%m-%dT%H:%M:%S')
            date_str_t1 = datetime.strftime(date_list[i + 1], '%Y-%m-%dT%H:%M:%S')
            get_tasks.append(gather_latency_stats(networkId=net_id,
                                                  t0=date_str_t0,
                                                  t1=date_str_t1,
                                                  filters=filters))
            get_tasks.append(gather_conn_stats(networkId=net_id, t0=date_str_t0,
                                               t1=date_str_t1, filters=filters))
    conn_stats_full = []
    latency_stats_full = []
    for task in asyncio.as_completed(get_tasks):
        stat_type, results = await task
        if stat_type == 'latency':
            latency_stats_full.append(results)
        elif stat_type == 'conn':
            conn_stats_full.append(results)

    return latency_stats_full, conn_stats_full


async def main(aiomeraki, net_id, date_list, filters):
    """
    Main async function wrapper
    :param aiomeraki: async library
    :param net_id: Network ID to fetch
    :param date_list: List of time intervals
    :param filters: Optional band, ssid and apTag filters
    :return: latency_stats_full, conn_stats_full: return stats to main execution
    """
    async with aiomeraki:
        latency_stats_full, conn_stats_full = await gather_wireless_stats(aiomeraki, net_id, date_list, filters)

    return latency_stats_full, conn_stats_full


if __name__ == "__main__":
    # -------------------Gather client count per band-------------------
    client_counts = dashboard.wireless.getNetworkWirelessClientCountHistory(networkId=net_id,
                                                                            t0=datetime.strftime(date_list[0],
                                                                                                 '%Y-%m-%dT%H:%M:%S'),
                                                                            t1=datetime.strftime(date_list[-1],
                                                                                                 '%Y-%m-%dT%H:%M:%S'),
                                                                            resolution=step, **filters_cc)
    client_counts_2 = dashboard.wireless.getNetworkWirelessClientCountHistory(networkId=net_id,
                                                                              t0=datetime.strftime(date_list[0],
                                                                                                   '%Y-%m-%dT%H:%M:%S'),
                                                                              t1=datetime.strftime(date_list[-1],
                                                                                                   '%Y-%m-%dT%H:%M:%S'),
                                                                              resolution=step, band="2.4",**filters_cc)
    client_counts_5 = dashboard.wireless.getNetworkWirelessClientCountHistory(networkId=net_id,
                                                                              t0=datetime.strftime(date_list[0],
                                                                                                   '%Y-%m-%dT%H:%M:%S'),
                                                                              t1=datetime.strftime(date_list[-1],
                                                                                                   '%Y-%m-%dT%H:%M:%S'),
                                                                              resolution=step, band="5", **filters_cc)
    client_counts_6 = dashboard.wireless.getNetworkWirelessClientCountHistory(networkId=net_id,
                                                                              t0=datetime.strftime(date_list[0],
                                                                                                   '%Y-%m-%dT%H:%M:%S'),
                                                                              t1=datetime.strftime(date_list[-1],
                                                                                                   '%Y-%m-%dT%H:%M:%S'),
                                                                              resolution=step, band="6", **filters_cc)

    # Start an event loop and fetch stats with async
    loop = asyncio.get_event_loop()
    latency_stats_full, conn_stats_full = loop.run_until_complete(main(aiomeraki, net_id, date_list, filters))

    # Format connection stats
    conn_stats_agg = []
    for stat_set in conn_stats_full:
        if stat_set['connStats'] != []:
            agg = {
                "startTs": stat_set['startTs'],
                "endTs": stat_set['endTs'],
                "successful_connections": stat_set['connStats']['success'],
                "total_failures": stat_set['connStats']['assoc'] + stat_set['connStats']['auth'] +
                                  stat_set['connStats']['dhcp'] + stat_set['connStats']['dns'],
                "assoc_failures": stat_set['connStats']['assoc'],
                "auth_failures": stat_set['connStats']['auth'],
                "dhcp_failures": stat_set['connStats']['dhcp'],
                "dns_failures": stat_set['connStats']['dns']
            }
        elif stat_set['connStats'] == []:
            agg = {
                "startTs": stat_set['startTs'],
                "endTs": stat_set['endTs'],
                "successful_connections": 0,
                "total_failures": 0,
                "assoc_failures": 0,
                "auth_failures": 0,
                "dhcp_failures": 0,
                "dns_failures": 0
            }
        conn_stats_agg.append(agg)

    # Format client counts
    for i in range(len(date_list)):
        if i < len(date_list) - 1:
            client_counts_agg = {
                'startTs': datetime.strftime(date_list[i],'%Y-%m-%dT%H:%M:%S'),
                'endTs': datetime.strftime(date_list[i+1], '%Y-%m-%dT%H:%M:%S'),
                'all_bands': client_counts[i]['clientCount'],
                '2.4GHz': client_counts_2[i]['clientCount'],
                '5GHz': client_counts_5[i]['clientCount'],
                '6GHz': client_counts_6[i]['clientCount']
            }
            client_counts_full.append(client_counts_agg)

    # Remove leading date, keep hour only
    for item in conn_stats_agg:
        item['startTs'] = item['startTs'].split('T')[1]
    for item in client_counts_full:
        item['startTs'] = item['startTs'].split('T')[1]
    for item in latency_stats_full:
        item['startTs'] = item['startTs'].split('T')[1]
    conn_stats_agg_df = pd.DataFrame(conn_stats_agg)
    client_counts_df = pd.DataFrame(client_counts_full)
    latency_stats_df = pd.DataFrame(latency_stats_full)

    if tag == "":
        tag = "All"
    if band == "":
        band = "All"
    if ssid == "":
        ssid = "All"

    # Convert to dataframes for plotting
    conn_stats_agg_df = conn_stats_agg_df.drop(columns="endTs")
    conn_stats_agg_df = conn_stats_agg_df.set_index("startTs").sort_index()
    conn_stats_time_steps = conn_stats_agg_df.index.to_numpy()
    conn_stats_agg_df.to_csv(f'./conn_stats_agg_df_{t0_str}_{num_days}day_Tag-{tag}_Band-{band}_SSID-{ssid}.csv')

    client_counts_df = client_counts_df.drop(columns="endTs")
    client_counts_df = client_counts_df.set_index("startTs")
    client_counts_time_steps = client_counts_df.index.to_numpy()
    client_counts_df.to_csv(f'./client_counts_df_{t0_str}_{num_days}day_Tag-{tag}_Band-{band}_SSID-{ssid}.csv')

    latency_stats_df = latency_stats_df.drop(columns="endTs")
    latency_stats_df = latency_stats_df.drop(columns="backgroundTraffic")
    latency_stats_df = latency_stats_df.drop(columns="bestEffortTraffic")
    latency_stats_df = latency_stats_df.drop(columns="videoTraffic")
    latency_stats_df = latency_stats_df.drop(columns="voiceTraffic")
    latency_stats_df = latency_stats_df.set_index("startTs").sort_index()
    latency_stats_time_steps = latency_stats_df.index.to_numpy()
    latency_stats_df.to_csv(f'./latency_stats_df_{t0_str}_{num_days}day_Tag-{tag}_Band-{band}_SSID-{ssid}.csv')

    # Plot
    plt.style.use('fivethirtyeight')
    conn_stats_agg_df.plot()
    plt.xticks(rotation=60, fontsize=12)
    plt.xlabel("Time")
    plt.ylabel("Total connection attempts")
    plt.title(f"{day}/{month}/{year} Connection Statistics - Tag: {tag} - Band: {band} - SSID: {ssid}", fontsize=18)
    plt.tight_layout()
    plt.show()
    # plt.savefig(f'./conn_stats_agg_df_{t0_str}_{num_days}day_AP_Tag-{tag}_Band-{band}_SSID-{ssid}.png', dpi=1200)

    plt.style.use('fivethirtyeight')
    client_counts_df.plot()
    plt.xticks(rotation=60, fontsize=12)
    plt.xlabel("Time")
    plt.ylabel("Clients")
    plt.title(f"{day}/{month}/{year} Total Clients - Tag: {tag} - Band: {band} - SSID: {ssid}", fontsize=18)
    plt.tight_layout()
    plt.show()
    # plt.savefig(f'./client_counts_df_{t0_str}_{num_days}day_Tag-{tag}_Band-{band}_SSID-{ssid}.png', dpi=1200)
    latency_stats_df.plot()
    plt.xticks(rotation=60, fontsize=12)
    plt.xlabel("Time")
    plt.ylabel("ms")
    plt.title(f"{day}/{month}/{year} Latency Statistics - Tag: {tag} - Band: {band} - SSID: {ssid}", fontsize=18)
    plt.tight_layout()
    plt.show()
    # plt.savefig(f'./latency_stats_df_{t0_str}_{num_days}day_Tag-{tag}_Band-{band}_SSID-{ssid}.png', dpi=1200)
