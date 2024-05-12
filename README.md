# Plan d'exécution (EXPLAIN) : Comparaison des coûts d'exécution de différentes requêtes

Supposons que nous ayons deux tables : "users" avec 1000 enregistrements et "orders" avec 1 million d'enregistrements. Nous voulons effectuer une jointure entre ces deux tables pour obtenir les commandes passées par chaque utilisateur.

## Requête 1 : Jointure entre une petite table et une très grande table

```sql
EXPLAIN SELECT * FROM users JOIN orders ON users.id = orders.user_id;
```

## Résultat du plan d'exécution :

    Utilisation d'une boucle imbriquée (Nested Loop Join).
    La petite table "users" est scannée une fois et les enregistrements correspondants sont recherchés dans la grande table "orders".

## Requête 2 : Jointure entre deux très grandes tables

sql

```sql

EXPLAIN SELECT * FROM orders o1 JOIN orders o2 ON o1.user_id = o2.user_id;
```
## Résultat du plan d'exécution :

    Utilisation d'une jointure par hachage (Hash Join).
    Les deux tables sont scannées et les enregistrements sont hashés pour effectuer la jointure.

Dans cet exemple, nous pouvons observer que le coût d'exécution de la première requête (jointure entre une petite table et une très grande table) est généralement inférieur au coût d'exécution de la deuxième requête (jointure entre deux très grandes tables).

# Plan d'exécution et index : Rôle des index dans les performances des requêtes

Supposons que nous ayons une table "cities" avec un grand nombre d'enregistrements et un attribut "population". Nous voulons lister les communes avec moins de 5000 habitants.
## Requête initiale sans index :

sql
```sql
EXPLAIN SELECT * FROM cities WHERE population < 5000;
```
## Résultat du plan d'exécution :

    Un scan séquentiel de la table est effectué pour récupérer tous les enregistrements, puis une sélection est appliquée pour filtrer les communes avec moins de 5000 habitants.

## Requête avec index :

sql
```sql

CREATE INDEX idx_population ON cities (population);
EXPLAIN SELECT * FROM cities WHERE population < 5000;
```
## Résultat du plan d'exécution :

    L'index sur l'attribut de population est utilisé pour effectuer une recherche sélective des communes avec moins de 5000 habitants, ce qui réduit le temps d'exécution global de la requête.

Dans cet exemple, nous pouvons observer que l'utilisation d'un index sur l'attribut de population améliore considérablement les performances de la requête en réduisant la quantité de données à parcourir.

#Transactions : Niveaux d'isolation et leur impact sur la cohérence des données

Supposons que nous ayons deux clients : un client en console et un client web. Les deux clients effectuent des modifications sur une table "products" qui contient des informations sur les produits.
Niveau d'isolation : Lecture non répétable

Client en console :

sql
```sql

START TRANSACTION;
SELECT * FROM products;
```
Client web :

sql
```sql

START TRANSACTION;
UPDATE products SET price = price + 10 WHERE category = 'Electronics';
COMMIT;
```
Dans ce cas, le client en console peut voir les modifications apportées par le client web avant la fin de la transaction.
Niveau d'isolation : Lecture répétable

Client en console :

sql
```sql

START TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SELECT * FROM products;
```
Client web :

sql
```sql

START TRANSACTION;
UPDATE products SET price = price + 10 WHERE category = 'Electronics';
COMMIT;
```
Dans ce cas, le client en console ne voit pas les modifications apportées par le client web avant la fin de la transaction.

Ces exemples illustrent comment les différents niveaux d'isolation (lecture non répétable, lecture répétable, lecture de commit, sérialisation) peuvent avoir un impact sur la cohérence des données et la visibilité des modifications effectuées par les transactions concurrentes.
