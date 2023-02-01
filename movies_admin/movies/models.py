import uuid
from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.db.models import UniqueConstraint
from django.utils.translation import gettext_lazy as _


class MovieType(models.TextChoices):
    movie = 'movie', _('Movie')
    tv_show = 'tv show', _('TV Show')


class WorkType(models.TextChoices):
    person_actor = 'actor', _('Actor')
    person_director = 'director', _('Director')
    person_writer = 'writer', _('Writer')


class TimeStampedMixin(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(_('Person name'), max_length=255)

    class Meta:
        db_table = "content\".\"person"
        verbose_name = _('Person')
        verbose_name_plural = _('Persons')
        indexes = [
            models.Index(fields=['id']),
        ]

    def __str__(self):
        return self.full_name


class Genre(UUIDMixin, TimeStampedMixin):
    name = models.CharField(_('Name'), max_length=255)
    description = models.TextField(_('Description'), blank=True)

    class Meta:
        db_table = "content\".\"genre"
        verbose_name = _('Genre')
        verbose_name_plural = _('Genres')

        indexes = [
            models.Index(fields=['id']),
        ]
        constraints = [
            models.UniqueConstraint(
                name='genre_idx',
                fields=['id', 'name'],)
        ]

    def __str__(self):
        return self.name


class Filmwork(UUIDMixin, TimeStampedMixin):

    title = models.CharField(_('Title'), max_length=255)
    description = models.TextField(_('Description'), blank=True)
    creation_date = models.DateTimeField(_('Creation date'),
                                         auto_now_add=True)
    rating = models.IntegerField(_('Rating'),
                                 blank=True,
                                 validators=[MinValueValidator(0),
                                             MaxValueValidator(100)])
    type = models.CharField(_('Movie type'),
                            choices=MovieType.choices,
                            max_length=7)
    genres = models.ManyToManyField(Genre, through='GenreFilmwork')
    roles = models.ManyToManyField(Person, through='PersonFilmwork')

    class Meta:
        db_table = "content\".\"film_work"
        verbose_name = _('Movie')
        verbose_name_plural = _('Movies')
        indexes = [
            models.Index(fields=['id']),
        ]
        constraints = [
            models.UniqueConstraint(
                name='filmwork_idx',
                fields=['id', 'title', 'creation_date', 'type'],)
        ]

    def __str__(self):
        return self.title


class GenreFilmwork(UUIDMixin):
    film_work = models.ForeignKey(Filmwork, on_delete=models.CASCADE)
    genre = models.ForeignKey(Genre, on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"genre_film_work"
        unique_together = (('genre', 'film_work'),)

        indexes = [
            models.Index(fields=['id']),
        ]

    def __str__(self):
        return str(self.genre)


class PersonFilmwork(UUIDMixin):
    film_work = models.ForeignKey(Filmwork, on_delete=models.CASCADE)
    person = models.ForeignKey(Person, on_delete=models.CASCADE)
    role = models.TextField(_('Role'), choices=WorkType.choices,
                            max_length=8)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"person_film_work"
        unique_together = (('film_work', 'person'),)

        indexes = [
            models.Index(fields=['id']),
        ]

    def __str__(self):
        return str(self.person)
