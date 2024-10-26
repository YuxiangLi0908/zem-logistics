from datetime import datetime, timedelta
from typing import Any

import pytz
import pandas as pd
from django.http import JsonResponse
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from asgiref.sync import sync_to_async,async_to_sync
from django.contrib.auth.decorators import login_required
from django.views import View
from django.utils.decorators import method_decorator
from django.utils import timezone
from django.db import models
from django.db.models import Sum, FloatField, IntegerField, Count, Case, When, F, Max, CharField, Value
from django.db.models.functions import Cast
from django.contrib.postgres.aggregates import StringAgg

from warehouse.models.order import Order
from warehouse.models.container import Container
from warehouse.models.packing_list import PackingList
from warehouse.models.po_check_eta import PoCheckEtaSeven
from warehouse.models.warehouse import ZemWarehouse
from warehouse.forms.warehouse_form import ZemWarehouseForm
from warehouse.views.export_file import export_po,export_po_check
from warehouse.forms.upload_file import UploadFileForm

@method_decorator(login_required(login_url='login'), name='dispatch')
class PO(View):
    template_main = "po/po.html"
    template_po_check_eta = "po/po_check_eta.html"
    template_po_check_retrieval = "po/po_check_retrieval.html"
    template_po_invalid = "po/po_invalid.html"
    template_po_list = "po/po_list.html"

    def get(self, request: HttpRequest) -> HttpResponse:
        step = request.GET.get("step")
        print('GET',step)
        if step == "po_check_eta": #到港前一周
            context = async_to_sync(self.handle_search_eta_seven)(request,"eta")
            return render(request, self.template_po_check_eta, context)
        elif step == "po_check_retrieval":#提柜前一天
            context = async_to_sync(self.handle_search_eta_seven)(request,"retrieval")
            return render(request, self.template_po_check_retrieval, context)
        elif step == "po_invalid":#PO失效
            context = async_to_sync(self.handle_po_check_seven)(request,"invalid")
            return render(request, self.template_po_invalid, context)
        elif step == "po_list":#PO总表
            context = async_to_sync(self.handle_po_check_seven)(request,"list")
            return render(request, self.template_po_list, context)
        else:
            current_date = datetime.now().date()
            start_date = current_date + timedelta(days=-30)
            end_date = current_date + timedelta(days=30)
            context = {
                "warehouse_form": ZemWarehouseForm(),
                "start_date": start_date.strftime('%Y-%m-%d'),
                "end_date": end_date.strftime('%Y-%m-%d'),
            }
            return render(request, self.template_main, context)
    
    def post(self, request: HttpRequest) -> HttpResponse:
        step = request.POST.get("step")
        print('POST',step)
        if step == "search":
            return render(request, self.template_main, self.handle_search_post(request))
        elif step == "selection":
            return render(request, self.template_main, self.handle_selection_post(request))
        elif step == "export_po":
            return export_po(request)
        elif step == "export_po_full":
            return export_po(request, "FULL_TABLE")
        elif step == "selection_check_seven":
            return export_po_check(request)
        elif step == "upload_check_po":  
            time_code= request.POST.get("time_code")
            context = async_to_sync(self.handle_upload_check_po_post)(request)
            if "eta" in time_code:
                return render(request, self.template_po_check_eta, context)
            elif "retrieval" in time_code:
                return render(request, self.template_po_check_retrieval, context)    
            elif "po_list" in time_code:
                return render(request, self.template_po_list, context)
        elif step == "po_invalid_save":
            context = async_to_sync(self.handle_upload_po_invalid_post)(request)
            return render(request, self.template_po_invalid, context)
    
    async def handle_upload_po_invalid_post(self, request: HttpRequest) -> tuple[Any]:
        ids = request.POST.getlist("po_ids")
        ids = [i.split(",") for i in ids]
        #是否确认通知客户
        selections = request.POST.getlist("is_selected")
        selected = [int(i) for s, id in zip(selections, ids) for i in id if s == "on"]
        text = request.POST.getlist("text")
        handing = [t for s, t in zip(selections, text) if s == "on"]
        #是否确认通知客户
        if selected:   
            for i in range(len(selected)):
                pochecketaseven = await sync_to_async(PoCheckEtaSeven.objects.get)(id = selected[i])
                pochecketaseven.is_notified = True
                pochecketaseven.handling_method = handing[i]
                await sync_to_async(pochecketaseven.save)()
        return await self.handle_po_check_seven(request,"invalid")

    async def handle_upload_check_po_post(self, request: HttpRequest) -> tuple[Any]:
        time_code= request.POST.get("time_code")
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            file = request.FILES['file']
            df = pd.read_csv(file)
            if 'shipping_mark' in df.columns and 'is_valid' in df.columns:
                data_pairs = [(row['BOL'], row['shipping_mark'], row['PRO'], row['PO List (use , as separator) *'], row['is_valid']) for index, row in df.iterrows()]
            else:
                print("Either 'PRO' or 'is_valid' column is not present in the DataFrame.")
        for bol,mark,fba,ref,is_valid in data_pairs:
            try:
                if bol:
                    #这里如果货柜表有重复会报错，应该不会有重复吧
                    container = await sync_to_async(Container.objects.get)(container_number = bol)
                    query = models.Q(container_number=container)
                    if mark:
                        query &= models.Q(shipping_mark=mark)
                    elif fba:
                        query &= models.Q(fba_id=fba)
                    elif ref:
                        query &= models.Q(ref_id=ref)
                    pochecketaseven = await sync_to_async(PoCheckEtaSeven.objects.get)(query)
                    pochecketaseven.status = is_valid == 1
                    cn = pytz.timezone('Asia/Shanghai')
                    current_time_cn = datetime.now(cn)
                    if "eta" in time_code:
                        pochecketaseven.last_eta_checktime = current_time_cn
                    elif "retrieval" in time_code:
                        pochecketaseven.last_retrieval_checktime = current_time_cn
                    await sync_to_async(pochecketaseven.save)()
            except PoCheckEtaSeven.DoesNotExist:
                continue
        return await self.handle_po_check_seven(request,time_code)
            
    async def handle_po_check_seven(self, request: HttpRequest, flag:str) -> dict[str, dict]:    
        if "eta" in flag:
            #如果是PO查验--到港前一周的
            po_checks = await sync_to_async(PoCheckEtaSeven.objects.filter)(time_status = True)
            #await sync_to_async(print)('多少条柜子eta',len(po_checks))
            po_checks_list = await sync_to_async(list)(po_checks)  
            await sync_to_async(print)('多少条柜子eta',len(po_checks_list))
            #先展示未查验的，然后按照vessel_eta排序        
            po_checks_list.sort(key = lambda po: (po.last_eta_checktime is not None, po.vessel_eta))
            context = {
                "po_check":po_checks_list,
                "upload_check_file": UploadFileForm(),
            }
        elif "retrieval" in flag:
            #如果是PO查验--提柜前一天的
            po_checks = await sync_to_async(PoCheckEtaSeven.objects.filter)(time_status = False)
            po_checks_list = await sync_to_async(list)(po_checks) 
            await sync_to_async(print)('多少条柜子retrieval',len(po_checks_list))          
            context = {
                "po_check":po_checks_list,
                "upload_check_file": UploadFileForm(),
            }
        elif "invalid" in flag:
            query = (models.Q(last_eta_checktime__isnull=False) | models.Q(last_retrieval_checktime__isnull=False)) & models.Q(status = False)
            po_checks = await sync_to_async(PoCheckEtaSeven.objects.filter)(query)
            po_checks_list = await sync_to_async(list)(po_checks)
            await sync_to_async(print)('多少条柜子invalid',len(po_checks_list))
            po_checks_list.sort(key = lambda po: po.is_notified)
            context = {
                "po_check":po_checks_list,
            }
        elif "list" in flag:
            #PO
            po_checks = await sync_to_async(PoCheckEtaSeven.objects.all)()            
            po_checks_list = await sync_to_async(list)(po_checks)       
            await sync_to_async(print)('多少条柜子list',len(po_checks_list))  
            context = {
                "po_check":po_checks_list,
            }
        return context
    
    async def handle_search_po_invalid(self, request: HttpRequest)-> tuple[str, dict[str, Any]]:
        po_checks = await sync_to_async(PoCheckEtaSeven.objects.all)()
        context = {
            "po_check":po_checks,
            }
        return context

    async def handle_search_eta_seven(self, request: HttpRequest, flag:str)-> tuple[str, dict[str, Any]]:
        seven_days_later = datetime.now().date() + timedelta(days=7)
        #筛选所有未来要查验的柜子，条件：1、vessel_eta小于一周后的，2、未取消预报的 。然后再分类
        query = models.Q(vessel_id__vessel_eta__lte = seven_days_later) & models.Q(cancel_notification__isnull=False)       
        #models.Q(retrieval_id__target_retrieval_timestamp__isnull=True)
        orders = await sync_to_async(list)(
            Order.objects.select_related(
                    'container_number', 'vessel_id','retrieval_id','offload_id'
                    ).filter(query)
            )
        
        po_checks = []
        for order in orders:
            await sync_to_async(print)('一轮筛选的柜子',order)
            container_number = order.container_number
            #根据柜号查找pls，因为第一次查询完pls，可能会有预报新加pl的情况
            pls = await sync_to_async(list)(PackingList.objects.filter(container_number = order.container_number))
            for pl in pls:
                try:
                    # 直接在查询集中查找是否存在具有相同container_number的对象
                    existing_obj = await sync_to_async(PoCheckEtaSeven.objects.get)(packing_list = pl)                  
                    if existing_obj.time_status:   
                        await sync_to_async(print)('表中查到了')
                        #如果查到了，继续判断是不是要将时间状态从到港前一周改为提柜前一天，条件是：有提柜计划，并且没有实际提柜或者实际提柜时间早于或等于今天
                        if order.retrieval_id and order.retrieval_id.target_retrieval_timestamp:
                            await sync_to_async(print)('有提柜计划')
                            #如果没有实际提柜
                            if not order.retrieval_id.actual_retrieval_timestamp:
                                await sync_to_async(print)('没有实际提柜')
                                existing_obj.time_status = False
                                await sync_to_async(existing_obj.save)()
                            else:
                                today = datetime.now()  #如果不筛选实际提柜早于明天，就会把港后的柜子全放进来
                                local_tz = pytz.timezone('Asia/Shanghai')
                                actual_ts = order.retrieval_id.actual_retrieval_timestamp.astimezone(local_tz).replace(tzinfo=None)
                                #如果是当天提柜
                                if actual_ts <= today + timedelta(days=1):
                                    await sync_to_async(print)('提柜时间比较早')
                                    #如果有提柜计划了就改时间状态
                                    existing_obj.time_status = False
                                    await sync_to_async(existing_obj.save)()
                except PoCheckEtaSeven.DoesNotExist:
                    await sync_to_async(print)('表中没找到')
                    #if ("/" not in pl.fba_id) or ("/" not in pl.ref_id):  #听建单的同事说，亚马逊的是一定有fba和ref，沃尔玛的一定有ref
                    if "/" not in pl.ref_id:      #所以如果ref_id没有，就是私人地址、UPS什么的
                        await sync_to_async(print)('需要进行拿约')
                        time_status = 0
                        if order.retrieval_id and order.retrieval_id.target_retrieval_timestamp:   #因为存在首次建表的情况，此时有的数据属于到港前一周，有的属于提柜前一天
                            await sync_to_async(print)("有提柜计划")
                            if not order.retrieval_id.actual_retrieval_timestamp:
                                await sync_to_async(print)('没有实际提柜')
                                po_check_dict = {
                                    'container_number': container_number,
                                    'vessel_eta': order.vessel_id.vessel_eta,
                                    'packing_list': pl,
                                    'time_status': False,
                                    'destination': pl.destination,
                                    'fba_id': pl.fba_id,
                                    'ref_id': pl.ref_id,
                                    'shipping_mark': pl.shipping_mark,
                                    #其他的字段用默认值
                                }
                                po_checks.append(po_check_dict)
                            else:
                                today = datetime.now()  #如果不筛选实际提柜早于明天，就会把港后的柜子全放进来
                                local_tz = pytz.timezone('Asia/Shanghai')
                                actual_ts = order.retrieval_id.actual_retrieval_timestamp.astimezone(local_tz).replace(tzinfo=None)
                                if actual_ts <= today + timedelta(days=1):
                                    await sync_to_async(print)('提柜时间比较早')                                 
                                    po_check_dict = {
                                        'container_number': container_number,
                                        'vessel_eta': order.vessel_id.vessel_eta,
                                        'packing_list': pl,
                                        'time_status': False,
                                        'destination': pl.destination,
                                        'fba_id': pl.fba_id,
                                        'ref_id': pl.ref_id,
                                        'shipping_mark': pl.shipping_mark,
                                        #其他的字段用默认值
                                    }
                                    po_checks.append(po_check_dict)
                                   
                        if order.retrieval_id and not order.retrieval_id.target_retrieval_timestamp:
                            await sync_to_async(print)('没有实际提柜')
                            po_check_dict = {
                                    'container_number': container_number,
                                    'vessel_eta': order.vessel_id.vessel_eta,
                                    'packing_list': pl,
                                    'time_status': True,
                                    'destination': pl.destination,
                                    'fba_id': pl.fba_id,
                                    'ref_id': pl.ref_id,
                                    'shipping_mark': pl.shipping_mark,
                                    #其他的字段用默认值
                                }
                            po_checks.append(po_check_dict)
                      
        await sync_to_async(PoCheckEtaSeven.objects.bulk_create)(
                PoCheckEtaSeven(**p) for p in po_checks
            )
        return await self.handle_po_check_seven(request,flag)
        


    def handle_search_post(self, request: HttpRequest) -> dict[str, Any]:
        warehouse = None if request.POST.get("name")=="N/A(直送)" else request.POST.get("name")
        try:
            warehouse_obj = ZemWarehouse.objects.get(name=warehouse)
        except:
            warehouse_obj = None
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        container_number = request.POST.get("container_number")
        container_list = container_number.split()
        criteria = models.Q(
            container_number__order__warehouse__name=warehouse,
            shipment_batch_number__isnull=True,
        )
        if container_list:
            criteria &= models.Q(container_number__container_number__in=container_list)
        if start_date:
            criteria &= (
                models.Q(container_number__order__eta__gte=start_date) |
                models.Q(container_number__order__vessel_id__vessel_eta__gte=start_date)
            )
        if end_date:
            criteria &= (
                models.Q(container_number__order__eta__lte=end_date) |
                models.Q(container_number__order__vessel_id__vessel_eta__lte=end_date)
            )
        packing_list = PackingList.objects.select_related(
            "container_number", "container_number__order", "container_number__order__warehouse"
        ).filter(criteria).annotate(
            str_id=Cast("id", CharField()),
        ).values(
            'fba_id', 'ref_id','address','zipcode','destination','delivery_method',
            'container_number__container_number',
        ).annotate(
            ids=StringAgg("str_id", delimiter=",", distinct=True),
            total_pcs=Sum(
                Case(
                    When(pallet__isnull=True, then=F("pcs")),
                    default=F("pallet__pcs"),
                    output_field=IntegerField()
                )
            ),
            total_cbm=Sum(
                Case(
                    When(pallet__isnull=True, then=F("cbm")),
                    default=F("pallet__cbm"),
                    output_field=FloatField()
                )
            ),
            total_weight_lbs=Sum(
                Case(
                    When(pallet__isnull=True, then=F("total_weight_lbs")),
                    default=F("pallet__weight_lbs"),
                    output_field=FloatField()
                )
            ),
            total_n_pallet_act=Count("pallet__pallet_id", distinct=True),
            total_n_pallet_est=Sum("cbm", output_field=FloatField())/2,
            label=Max(
                Case(
                    When(pallet__isnull=True, then=Value("EST")),
                    default=Value("ACT"),
                    output_field=CharField()
                )
            ),
        ).distinct().order_by("destination", "container_number__container_number")
        context = {
            "packing_list": packing_list,
            "warehouse_form": ZemWarehouseForm(initial={"name": request.POST.get("name")}),
            "name": request.POST.get("name"),
            "warehouse": warehouse_obj,
            "start_date": start_date,
            "end_date": end_date,
            "container_number": container_number,
        }
        return context
    

    def handle_selection_post(self, request: HttpRequest) -> HttpResponse:
        warehouse = request.POST.get("warehouse")
        ids = request.POST.getlist("pl_ids")
        ids = [i.split(",") for i in ids]
        selections = request.POST.getlist("is_selected")
        selected = [int(i) for s, id in zip(selections, ids) for i in id if s == "on"]
        if selected:
            packing_list = PackingList.objects.select_related(
                "container_number", "pallet"
            ).filter(
                id__in=selected
            ).values(
                'fba_id', 'ref_id','address','zipcode','destination','delivery_method',
                'container_number__container_number',
            ).annotate(
                total_pcs=Sum(
                    Case(
                        When(pallet__isnull=True, then=F("pcs")),
                        default=F("pallet__pcs"),
                        output_field=IntegerField()
                    )
                ),
                total_cbm=Sum(
                    Case(
                        When(pallet__isnull=True, then=F("cbm")),
                        default=F("pallet__cbm"),
                        output_field=FloatField()
                    )
                ),
                total_weight_lbs=Sum(
                    Case(
                        When(pallet__isnull=True, then=F("total_weight_lbs")),
                        default=F("pallet__weight_lbs"),
                        output_field=FloatField()
                    )
                ),
                total_n_pallet_act=Count("pallet__pallet_id", distinct=True),
                total_n_pallet_est=Sum("cbm", output_field=FloatField())/2,
                label=Max(
                    Case(
                        When(pallet__isnull=True, then=Value("EST")),
                        default=Value("ACT"),
                        output_field=CharField()
                    )
                ),
            ).distinct().order_by("destination", "container_number__container_number")
            agg_pl = PackingList.objects.filter(
                id__in=selected
            ).annotate(
                label=Case(
                        When(pallet__isnull=True, then=Value("EST")),
                        default=Value("ACT"),
                        output_field=CharField()
                    )
            ).values("label").annotate(
                total_pcs=Sum(
                    Case(
                        When(pallet__isnull=True, then=F("pcs")),
                        default=F("pallet__pcs"),
                        output_field=IntegerField()
                    )
                ),
                total_cbm=Sum(
                    Case(
                        When(pallet__isnull=True, then=F("cbm")),
                        default=F("pallet__cbm"),
                        output_field=FloatField()
                    )
                ),
                total_weight_lbs=Sum(
                    Case(
                        When(pallet__isnull=True, then=F("total_weight_lbs")),
                        default=F("pallet__weight_lbs"),
                        output_field=FloatField()
                    )
                ),
                total_n_pallet_act=Count("pallet__pallet_id", distinct=True),
                total_n_pallet_est=Sum("cbm", output_field=FloatField())/2,
                
            ).order_by("label")
            
            summary = self._get_pl_agg_summary(agg_pl)
            context = {
                "selected_packing_list": packing_list,
                "selected_pl_ids": selected,
                "warehouse": warehouse,
                "warehouse_form": ZemWarehouseForm(initial={"name": warehouse}),
                "summary": summary,
            }
            return context
        else:
            mutable_post = request.POST.copy()
            mutable_post['name'] = warehouse
            request.POST = mutable_post
            return self.handle_search_post(request)
        
    def _get_pl_agg_summary(self, agg_pl: Any) -> dict[str, Any]:
        if len(agg_pl) == 2:
            est_pallet = agg_pl[1]["total_n_pallet_est"]//1 + (1 if agg_pl[1]["total_n_pallet_est"]%1 >= 0.45 else 0)
            return {
                "total_cbm": agg_pl[0]["total_cbm"] + agg_pl[1]["total_cbm"],
                "total_pcs": agg_pl[0]["total_pcs"] + agg_pl[1]["total_pcs"],
                "total_weight_lbs": agg_pl[0]["total_weight_lbs"] + agg_pl[1]["total_weight_lbs"],
                "act_pallet": agg_pl[0]["total_n_pallet_act"],
                "est_pallet": est_pallet,
                "total_pallet": agg_pl[0]["total_n_pallet_act"] + est_pallet,
            }
        elif agg_pl[0]["label"] == "EST":
            est_pallet = agg_pl[0]["total_n_pallet_est"]//1 + (1 if agg_pl[0]["total_n_pallet_est"]%1 >= 0.45 else 0)
            return {
                "total_cbm": agg_pl[0]["total_cbm"],
                "total_pcs": agg_pl[0]["total_pcs"],
                "total_weight_lbs": agg_pl[0]["total_weight_lbs"],
                "act_pallet": 0,
                "est_pallet": est_pallet,
                "total_pallet": est_pallet,
            }
        else:
            return {
                "total_cbm": agg_pl[0]["total_cbm"],
                "total_pcs": agg_pl[0]["total_pcs"],
                "total_weight_lbs": agg_pl[0]["total_weight_lbs"],
                "act_pallet": agg_pl[0]["total_n_pallet_act"],
                "est_pallet": 0,
                "total_pallet": agg_pl[0]["total_n_pallet_act"],
            }
        