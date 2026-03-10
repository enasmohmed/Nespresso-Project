
from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone


# Create your models here.


class UploadedFile(models.Model):
    file = models.FileField(upload_to='uploads/')
    uploaded_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.file.name} ({self.uploaded_at:%Y-%m-%d %H:%M})"




class UploadMonth(models.Model):
    month = models.CharField(max_length=20, unique=True)

    def __str__(self):
        return self.month



class ExcelSheetCache(models.Model):
    """كاش بيانات شيت إكسل: يُملأ عند الرفع ويُستخدم لتسريع فتح التابات."""
    sheet_name = models.CharField(max_length=255, unique=True, db_index=True)
    data = models.JSONField(default=list)  # list of dicts (rows)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Excel Sheet Cache"
        verbose_name_plural = "Excel Sheet Caches"

    def __str__(self):
        return f"{self.sheet_name} ({len(self.data)} rows)"


class MeetingPoint(models.Model):
    description = models.TextField()  # لازم يكون TextField أو CharField
    is_done = models.BooleanField(default=False)
    created_at = models.DateField(default=timezone.now)
    target_date = models.DateField(null=True, blank=True)
    assigned_to = models.CharField(max_length=255, blank=True, null=True)

    # def save(self, *args, **kwargs):
    #     # لو مفيش تاريخ هدف، حطيه بعد 7 أيام من الإنشاء
    #     if not self.target_date and not self.pk:
    #         from datetime import date
    #         self.target_date = date.today() + timedelta(days=7)
    #     super().save(*args, **kwargs)

    def __str__(self):
        return self.description[:50]
