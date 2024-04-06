Strategie 1:
1. Buffer A erstellen und vollmachen
2. Einen nächsten Buffer B erstellen
3. Die erste Zeile in Buffer B finden
3. Diese Zeile zum vorherigen Buffer A geben
4. Buffer A einem neuen Thread geben, der den Buffer bearbeitet
5. Buffer B zu Buffer A umbenennen
6. zu Schritt 2

Verbesserungen:
IEEE runden

Strategie 2:
1. Ein Struct, das die File und einen Vector von Buffern enthält
2. Das Struct ist in einem Mutex und jeder Thread kann jeweils einen neuen Buffer
anfragen
3. Im Struct läuft die Strategie 1 ab; hier können dann noch weitere Optimierungen vorgenommen werden