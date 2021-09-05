import datetime
from typing import Optional, Tuple

from nonebot import CommandSession
from peewee import Case, SQL, fn

from hoshino import Service
from hoshino.modules.yobot.yobot.src.client.ybplugins.ybdata import Clan_challenge

sv = Service('出刀数统计')


def date_str_datetime(date: str) -> Optional[datetime.datetime]:
	"""
	时间转datetime.datetime对象
	接受以下输入:
	YYYY/MM/DD
	"""
	date = date.split("/")
	try:
		date_obj = datetime.datetime(year=int(date[0]), month=int(date[1]), day=int(date[2]), hour=23, minute=59,
		                             second=59)
		return date_obj
	except:
		return None


async def query_challenge_nums(gid: int, end_date: Optional[datetime.datetime] = datetime.datetime.now()) -> Tuple[
	list, dict]:
	if not end_date:
		end_date = datetime.datetime.now()
	if type(end_date) != datetime.datetime or type(gid) != int:
		raise TypeError
	
	own_challenge = fn.SUM(Case(None,
	                            (
		                            ((Clan_challenge.is_continue == True) & (Clan_challenge.behalf.is_null(True)), 0.5),
		                            ((Clan_challenge.is_continue == False) & (Clan_challenge.behalf.is_null(True)), 1)
	                            ), 0))
	is_behaved_challenge = fn.SUM(Case(None,
	                                   (
		                                   ((Clan_challenge.is_continue == True) & (
			                                   Clan_challenge.behalf.is_null(False)),
		                                    0.5),
		                                   ((Clan_challenge.is_continue == False) & (
			                                   Clan_challenge.behalf.is_null(False)),
		                                    1)
	                                   ), 0))
	total_challenge = fn.SUM(Case(None,
	                              (
		                              (Clan_challenge.is_continue == True, 0.5),
		                              (Clan_challenge.is_continue == False, 1)
	                              ), 0))
	
	challenge_query = Clan_challenge.select(
		Clan_challenge.qqid,
		own_challenge.alias('own_challenge'),
		is_behaved_challenge.alias('is_behaved_challenge'),
		total_challenge.alias('total_challenge')
	).where(
		Clan_challenge.gid == int(gid),
		Clan_challenge.challenge_pcrdate >= int(
			divmod((end_date - datetime.timedelta(days=5, hours=5)).timestamp(), 86400)[0]),
		Clan_challenge.challenge_pcrdate <= int(divmod((end_date - datetime.timedelta(hours=5)).timestamp(), 86400)[0])
	).group_by(
		Clan_challenge.qqid
	).order_by(
		SQL('total_challenge').desc()
	).dicts()
	
	behalf_challenge = Clan_challenge.select(
		Clan_challenge.behalf,
		total_challenge.alias('total_challenge')
	).where(
		Clan_challenge.gid == int(gid),
		Clan_challenge.challenge_pcrdate >= int(
			divmod((end_date - datetime.timedelta(days=5, hours=5)).timestamp(), 86400)[0]),
		Clan_challenge.challenge_pcrdate <= int(divmod((end_date - datetime.timedelta(hours=5)).timestamp(), 86400)[0]),
		Clan_challenge.behalf.is_null(False)
	).group_by(
		Clan_challenge.behalf
	).order_by(
		SQL('total_challenge').desc()
	).dicts()
	
	behalf_challenge_dict = {x["behalf"]: x["total_challenge"] for x in behalf_challenge}
	
	return [x for x in challenge_query], behalf_challenge_dict


@sv.on_command('出刀统计', only_to_me=False)
async def _get_challenge_num(session: CommandSession):
	arg = session.current_arg.strip()
	if arg and not (end_date := date_str_datetime(arg)):
		session.finish("日期格式错误，请按：2021/9/2 格式进行输入")
	own_challenge, behalf_challenge = await query_challenge_nums(session.event.group_id, end_date if arg else None)
	if not own_challenge and not behalf_challenge:
		session.finish("本群此五天无出刀记录，请使用”出刀统计 日期“来手动指定会战结束时间进行查询（如：出刀统计 2021/9/2）")
	try:
		member_info = await session.bot.get_group_member_list(group_id=session.event.group_id,
		                                                      self_id=session.event.self_id)
		member_name = {x["user_id"]: x["nickname"] for x in member_info}
	except Exception:
		member_name = {}
	message = '出刀情况\n'
	for member in own_challenge:
		message += f'{member["qqid"]}({member_name.get(member["qqid"], "未知")})\n 他人代报{member["is_behaved_challenge"]}刀 ' \
		           f' 代报他人{behalf_challenge.get(member["qqid"], 0)}刀 自报{member["own_challenge"]}刀' \
		           f' 合计{member["total_challenge"] + behalf_challenge.get(member["qqid"], 0)}刀\n'
	message += '若统计不全，请使用”出刀统计 日期“来手动指定会战结束时间进行查询（如：出刀统计 2021/9/2）'
	session.finish(message)
