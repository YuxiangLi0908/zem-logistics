"""Split historical comparison IDs without changing quote items or their prices."""
from decimal import InvalidOperation

from django.db import migrations, transaction
from django.db.models import Count, Min

from warehouse.utils.quote_identity_v1 import digest
from warehouse.utils.quote_identity_v2 import ADDRESS_GROUPS, profile_configuration, scope_configuration


def remap_members(Member, alias, source_id, target_id):
    """Keep saved cargo order, collapsing only references to the same resulting AQ."""
    for member in Member.objects.using(alias).filter(profile_id=source_id).order_by("group_id", "position"):
        existing = Member.objects.using(alias).filter(group_id=member.group_id, profile_id=target_id).first()
        if existing:
            if existing.position < member.position:
                Member.objects.using(alias).filter(pk=member.pk).delete()
            else:
                Member.objects.using(alias).filter(pk=existing.pk).delete()
                Member.objects.using(alias).filter(pk=member.pk).update(profile_id=target_id)
            for position, pk in enumerate(Member.objects.using(alias).filter(group_id=member.group_id)
                                          .order_by("position").values_list("pk", flat=True), 1):
                Member.objects.using(alias).filter(pk=pk).update(position=position)
        else:
            Member.objects.using(alias).filter(pk=member.pk).update(profile_id=target_id)


def split_profiles(apps, schema_editor):
    alias = schema_editor.connection.alias
    Batch = apps.get_model("warehouse", "AutoQuoteBatch")
    Profile = apps.get_model("warehouse", "AutoQuoteProfile")
    Member = apps.get_model("warehouse", "AutoQuoteCargoGroupMember")
    WorkerState = apps.get_model("warehouse", "AutoQuoteWorkerState")
    changed, moved, unassigned = 0, 0, 0
    with transaction.atomic(using=alias):
        # Serialize against task creation and worker completion while rebinding IDs.
        WorkerState.objects.using(alias).get_or_create(pk=1)
        WorkerState.objects.using(alias).select_for_update().get(pk=1)
        profiles = list(Profile.objects.using(alias).order_by("id"))
        for profile in profiles:
            groups = list(Batch.objects.using(alias).filter(profile_id=profile.pk).order_by().values("group")
                          .annotate(first_id=Min("id"), tasks=Count("id")).order_by("first_id"))
            if not groups:
                # A cargo-only template has no historical destination group to infer.
                # It remains usable; starting it resolves an AQ for the selected group.
                continue
            if any(row["group"] not in ADDRESS_GROUPS for row in groups):
                raise ValueError(f"AQ{profile.pk:06d} 的历史任务地址组不正确，编号拆分已回滚")
            canonical = profile.configuration.get("address_group")
            if canonical not in ADDRESS_GROUPS:
                canonical = groups[0]["group"]
            mapping, canonical_id = [], profile.pk
            profile_changed = False
            for row in groups:
                group = row["group"]
                configuration = scope_configuration(profile.configuration, group)
                fingerprint = digest(configuration)
                target = Profile.objects.using(alias).filter(fingerprint=fingerprint).first()
                if target is None and group == canonical:
                    Profile.objects.using(alias).filter(pk=profile.pk).update(
                        fingerprint=fingerprint, configuration=configuration)
                    target = profile
                    profile_changed = True
                elif target is None:
                    target = Profile.objects.using(alias).create(
                        fingerprint=fingerprint, configuration=configuration, origin_label=profile.origin_label)
                if group == canonical:
                    canonical_id = target.pk
                if target.pk != profile.pk:
                    moved += Batch.objects.using(alias).filter(profile_id=profile.pk, group=group).update(profile_id=target.pk)
                    profile_changed = True
                mapping.append(f'{group}:AQ{target.pk:06d}（{row["tasks"]}个任务）')
            if canonical_id != profile.pk:
                remap_members(Member, alias, profile.pk, canonical_id)
            if profile_changed:
                changed += 1
                print(f'自动询价编号 AQ{profile.pk:06d} -> {"；".join(mapping)}')

        for batch in Batch.objects.using(alias).filter(profile__isnull=True).iterator(chunk_size=100):
            try:
                configuration = profile_configuration(batch.parameters, batch.group)
            except (ValueError, TypeError, KeyError, InvalidOperation):
                unassigned += 1
                print(f"自动询价任务 #{batch.pk} 参数不完整，保留原始数据，暂不分配AQ编号")
                continue
            profile, _ = Profile.objects.using(alias).get_or_create(fingerprint=digest(configuration), defaults={
                "configuration": configuration, "origin_label": str(batch.parameters.get("originWarehouse") or "")[:200]})
            Batch.objects.using(alias).filter(pk=batch.pk, profile__isnull=True).update(profile_id=profile.pk)
            moved += 1

        shared = Batch.objects.using(alias).exclude(profile__isnull=True).order_by().values("profile_id") \
            .annotate(group_count=Count("group", distinct=True)).filter(group_count__gt=1)
        if shared.exists():
            raise ValueError("仍有AQ编号被多个地址组共用，编号拆分已回滚")
    print(f"自动询价编号检查完成：调整{changed}个旧编号，重关联{moved}个任务，未分配编号{unassigned}个任务。")


class Migration(migrations.Migration):
    dependencies = [("warehouse", "0404_auto_quote_cargo_groups")]
    operations = [migrations.RunPython(split_profiles)]
