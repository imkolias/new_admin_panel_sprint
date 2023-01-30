from django.contrib import admin
from .models import Genre, Filmwork, Person, GenreFilmwork, PersonFilmwork


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork
    autocomplete_fields = ('genre', 'film_work',)


class PersonFilmworkInline(admin.TabularInline):
    model = PersonFilmwork
    autocomplete_fields = ('person', 'film_work',)


@admin.register(Genre)
class Genre(admin.ModelAdmin):
    inlines = (GenreFilmworkInline,)
    list_display = ('name',)
    ordering = ('name',)
    search_fields = ('name',)


@admin.register(Person)
class Person(admin.ModelAdmin):
    inlines = (PersonFilmworkInline,)
    list_display = ('full_name',)
    ordering = ('full_name',)
    search_fields = ('full_name',)



@admin.register(Filmwork)
class FilmWork(admin.ModelAdmin):
    inlines = (GenreFilmworkInline, PersonFilmworkInline)

    list_display = ('title', 'type', 'creation_date', 'rating', )
    list_filter = ('type', 'creation_date',)
    ordering = ('title',)
    search_fields = ('title', 'description', 'id')



